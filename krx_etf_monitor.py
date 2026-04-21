from __future__ import annotations

import argparse
import getpass
import html
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

KST = ZoneInfo("Asia/Seoul")
os.environ.setdefault("MPLCONFIGDIR", str(Path(".matplotlib-cache").resolve()))
KRX_CREDENTIAL_SERVICE = "se2in-etf-monitor-krx"

DEFAULT_CONFIG: dict[str, Any] = {
    "database_path": "data/krx_active_etf_holdings.sqlite",
    "watchlist": [],
    "include_keywords": ["액티브"],
    "exclude_keywords": [],
    "min_weight_delta_pp": 0.05,
    "max_etfs_in_telegram": 30,
    "max_changes_per_etf": 10,
}


@dataclass(frozen=True)
class Etf:
    ticker: str
    name: str
    isin: str = ""


@dataclass(frozen=True)
class Holding:
    etf_ticker: str
    trade_date: str
    holding_code: str
    holding_name: str
    quantity: float | None
    amount: float | None
    weight: float | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class Change:
    trade_date: str
    etf_ticker: str
    etf_name: str
    holding_code: str
    holding_name: str
    previous_weight: float | None
    current_weight: float | None
    weight_delta: float
    change_type: str


def load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_config(path: str | None) -> dict[str, Any]:
    config = dict(DEFAULT_CONFIG)
    if path and Path(path).exists():
        with open(path, "r", encoding="utf-8-sig") as f:
            config.update(json.load(f))
    return config


def normalize_date(value: str | None) -> str:
    if value is None:
        return datetime.now(KST).strftime("%Y%m%d")
    value = value.strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y%m%d")
        except ValueError:
            pass
    raise ValueError("date must be YYYYMMDD or YYYY-MM-DD")


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "nan", "NaN", "None"}:
        return None
    text = text.replace(",", "").replace("%", "")
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def setup_krx_login() -> int:
    import keyring

    krx_id = input("KRX market data ID: ").strip()
    krx_pw = getpass.getpass("KRX market data password: ")
    if not krx_id or not krx_pw:
        raise SystemExit("ID and password are required.")
    keyring.set_password(KRX_CREDENTIAL_SERVICE, "KRX_ID", krx_id)
    keyring.set_password(KRX_CREDENTIAL_SERVICE, "KRX_PW", krx_pw)
    print("KRX login was saved to Windows Credential Manager.")
    return 0


def load_krx_credentials() -> None:
    load_env_file()
    if os.environ.get("KRX_ID") and os.environ.get("KRX_PW"):
        return
    try:
        import keyring

        krx_id = keyring.get_password(KRX_CREDENTIAL_SERVICE, "KRX_ID")
        krx_pw = keyring.get_password(KRX_CREDENTIAL_SERVICE, "KRX_PW")
    except Exception:
        krx_id = None
        krx_pw = None
    if krx_id and krx_pw:
        os.environ.setdefault("KRX_ID", krx_id)
        os.environ.setdefault("KRX_PW", krx_pw)
        return
    raise RuntimeError(
        "KRX login is not set. Run setup_krx_login.bat first. "
        "Do not paste your password into chat."
    )


def get_pykrx_stock():
    load_krx_credentials()
    try:
        from pykrx import stock
    except ImportError as exc:
        raise RuntimeError("pykrx is not installed. Run: python -m pip install -r requirements.txt") from exc
    return stock


def open_db(database_path: str) -> sqlite3.Connection:
    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS etfs (
            ticker TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            isin TEXT,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            active_detected_by TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,
            message TEXT
        );

        CREATE TABLE IF NOT EXISTS holdings (
            trade_date TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            holding_code TEXT NOT NULL,
            holding_name TEXT NOT NULL,
            quantity REAL,
            amount REAL,
            weight REAL,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (trade_date, etf_ticker, holding_code)
        );

        CREATE INDEX IF NOT EXISTS idx_holdings_etf_date
            ON holdings (etf_ticker, trade_date);

        CREATE TABLE IF NOT EXISTS changes (
            run_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            holding_code TEXT NOT NULL,
            holding_name TEXT NOT NULL,
            previous_weight REAL,
            current_weight REAL,
            weight_delta REAL NOT NULL,
            change_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (run_id, etf_ticker, holding_code),
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        );
        """
    )
    ensure_column(conn, "etfs", "isin", "TEXT")
    conn.commit()


class KrxClient:
    def __init__(self) -> None:
        self.stock = get_pykrx_stock()

    def nearest_trade_date(self, date: str | None) -> str:
        current = datetime.strptime(normalize_date(date), "%Y%m%d")
        for _ in range(14):
            trial = current.strftime("%Y%m%d")
            try:
                if self.stock.get_etf_ticker_list(trial):
                    return trial
            except Exception:
                pass
            current -= timedelta(days=1)
        raise RuntimeError("Could not find a recent KRX trading date.")

    def etf_name(self, ticker: str) -> str:
        try:
            return str(self.stock.get_etf_ticker_name(ticker))
        except Exception:
            return ticker

    def etf_isin(self, ticker: str) -> str:
        try:
            return str(self.stock.get_etf_isin(ticker))
        except Exception:
            return ""

    def discover_active_etfs(self, config: dict[str, Any], date: str | None) -> tuple[str, list[Etf]]:
        trade_date = self.nearest_trade_date(date)
        watchlist = {str(ticker).zfill(6) for ticker in config.get("watchlist", []) if str(ticker).strip()}
        include_keywords = [str(v) for v in config.get("include_keywords", ["액티브"]) if str(v)]
        exclude_keywords = [str(v) for v in config.get("exclude_keywords", []) if str(v)]
        if not include_keywords and not watchlist:
            include_keywords = ["액티브"]

        etfs: list[Etf] = []
        for ticker in self.stock.get_etf_ticker_list(trade_date):
            ticker = str(ticker).zfill(6)
            name = self.etf_name(ticker)
            if watchlist and ticker not in watchlist:
                continue
            if not watchlist and include_keywords and not any(keyword in name for keyword in include_keywords):
                continue
            if exclude_keywords and any(keyword in name for keyword in exclude_keywords):
                continue
            etfs.append(Etf(ticker=ticker, name=name, isin=self.etf_isin(ticker)))
        return trade_date, sorted(etfs, key=lambda item: (item.name, item.ticker))

    def fetch_holdings(self, etf: Etf, date: str | None) -> tuple[str, list[Holding]]:
        trade_date = self.nearest_trade_date(date)
        df = self.stock.get_etf_portfolio_deposit_file(etf.ticker, trade_date)
        if df is None or df.empty:
            return trade_date, []
        records = df.reset_index().to_dict(orient="records")
        columns = [str(col) for col in df.reset_index().columns]
        rows: list[Holding] = []
        seen_codes: set[str] = set()
        for idx, row in enumerate(records, start=1):
            raw = {str(k): v for k, v in row.items()}
            values_by_pos = [raw.get(col) for col in columns]
            code = first_value(raw, ["티커", "종목코드", "COMPST_ISU_CD", "index"])
            name = first_value(raw, ["구성종목명", "종목명", "COMPST_ISU_NM"])
            quantity = first_value(raw, ["계약수", "수량", "COMPST_ISU_CU1_SHRS"])
            amount = first_value(raw, ["금액", "평가금액", "VALU_AMT"])
            weight = first_value(raw, ["비중", "비중(%)", "COMPST_RTO"])

            if code is None and values_by_pos:
                code = values_by_pos[0]
            if name is None and len(values_by_pos) > 1:
                name = values_by_pos[1]
            if quantity is None and len(values_by_pos) > 2:
                quantity = values_by_pos[2]
            if amount is None and len(values_by_pos) > 3:
                amount = values_by_pos[3]
            if weight is None and values_by_pos:
                weight = values_by_pos[-1]

            holding_code = str(code or name or f"ROW{idx}").strip()
            if len(holding_code) > 6 and holding_code.startswith("KR"):
                holding_code = holding_code[3:9]
            if holding_code in seen_codes:
                holding_code = f"{holding_code}_{idx}"
            seen_codes.add(holding_code)
            rows.append(
                Holding(
                    etf_ticker=etf.ticker,
                    trade_date=trade_date,
                    holding_code=holding_code,
                    holding_name=str(name or holding_code).strip(),
                    quantity=parse_number(quantity),
                    amount=parse_number(amount),
                    weight=parse_number(weight),
                    raw=raw,
                )
            )
        return trade_date, rows


def first_value(row: dict[str, Any], candidates: list[str]) -> Any:
    for key in candidates:
        if key in row:
            return row[key]
    return None


def upsert_etfs(conn: sqlite3.Connection, etfs: list[Etf], trade_date: str) -> None:
    now = datetime.now(KST).isoformat(timespec="seconds")
    for etf in etfs:
        conn.execute(
            """
            INSERT INTO etfs (ticker, name, isin, first_seen_date, last_seen_date, active_detected_by, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                name = excluded.name,
                isin = excluded.isin,
                last_seen_date = excluded.last_seen_date,
                updated_at = excluded.updated_at
            """,
            (etf.ticker, etf.name, etf.isin, trade_date, trade_date, "krx_name_keyword", now),
        )
    conn.commit()


def replace_holdings(conn: sqlite3.Connection, etf: Etf, trade_date: str, holdings: list[Holding]) -> None:
    now = datetime.now(KST).isoformat(timespec="seconds")
    conn.execute("DELETE FROM holdings WHERE trade_date = ? AND etf_ticker = ?", (trade_date, etf.ticker))
    conn.executemany(
        """
        INSERT INTO holdings (
            trade_date, etf_ticker, holding_code, holding_name, quantity, amount, weight, raw_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                item.trade_date,
                item.etf_ticker,
                item.holding_code,
                item.holding_name,
                item.quantity,
                item.amount,
                item.weight,
                json.dumps(item.raw, ensure_ascii=False, default=str),
                now,
            )
            for item in holdings
        ],
    )
    conn.commit()


def previous_trade_date_in_db(conn: sqlite3.Connection, etf_ticker: str, trade_date: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(trade_date) AS trade_date FROM holdings WHERE etf_ticker = ? AND trade_date < ?",
        (etf_ticker, trade_date),
    ).fetchone()
    return row["trade_date"] if row and row["trade_date"] else None


def load_holdings_from_db(conn: sqlite3.Connection, etf_ticker: str, trade_date: str) -> dict[str, sqlite3.Row]:
    rows = conn.execute(
        "SELECT holding_code, holding_name, weight FROM holdings WHERE etf_ticker = ? AND trade_date = ?",
        (etf_ticker, trade_date),
    ).fetchall()
    return {row["holding_code"]: row for row in rows}


def compare_holdings(etf: Etf, trade_date: str, previous: dict[str, sqlite3.Row], current: dict[str, sqlite3.Row], min_delta: float) -> list[Change]:
    changes: list[Change] = []
    for code in set(previous) | set(current):
        prev = previous.get(code)
        curr = current.get(code)
        prev_weight = prev["weight"] if prev else None
        curr_weight = curr["weight"] if curr else None
        delta = round(float(curr_weight or 0.0) - float(prev_weight or 0.0), 6)
        if prev is None:
            change_type = "ADDED"
        elif curr is None:
            change_type = "REMOVED"
        elif abs(delta) >= min_delta:
            change_type = "CHANGED"
        else:
            continue
        changes.append(
            Change(
                trade_date=trade_date,
                etf_ticker=etf.ticker,
                etf_name=etf.name,
                holding_code=code,
                holding_name=(curr or prev)["holding_name"],
                previous_weight=prev_weight,
                current_weight=curr_weight,
                weight_delta=delta,
                change_type=change_type,
            )
        )
    return sorted(changes, key=lambda item: abs(item.weight_delta), reverse=True)


def start_run(conn: sqlite3.Connection, trade_date: str) -> int:
    now = datetime.now(KST).isoformat(timespec="seconds")
    cur = conn.execute("INSERT INTO runs (trade_date, started_at, status) VALUES (?, ?, ?)", (trade_date, now, "RUNNING"))
    conn.commit()
    return int(cur.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, status: str, message: str = "") -> None:
    now = datetime.now(KST).isoformat(timespec="seconds")
    conn.execute("UPDATE runs SET ended_at = ?, status = ?, message = ? WHERE id = ?", (now, status, message, run_id))
    conn.commit()


def save_changes(conn: sqlite3.Connection, run_id: int, changes: list[Change]) -> None:
    now = datetime.now(KST).isoformat(timespec="seconds")
    conn.executemany(
        """
        INSERT OR REPLACE INTO changes (
            run_id, trade_date, etf_ticker, holding_code, holding_name,
            previous_weight, current_weight, weight_delta, change_type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                item.trade_date,
                item.etf_ticker,
                item.holding_code,
                item.holding_name,
                item.previous_weight,
                item.current_weight,
                item.weight_delta,
                item.change_type,
                now,
            )
            for item in changes
        ],
    )
    conn.commit()


def format_weight(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}%"


def format_delta(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}pp"


def build_report(trade_date: str, etfs: list[Etf], changes_by_etf: dict[str, list[Change]], skipped: list[str], config: dict[str, Any]) -> str:
    pretty_date = datetime.strptime(trade_date, "%Y%m%d").strftime("%Y-%m-%d")
    changed_etfs = [etf for etf in etfs if changes_by_etf.get(etf.ticker)]
    buy_limit = int(config.get("max_buy_per_etf", 10))
    sell_limit = int(config.get("max_sell_per_etf", 10))
    max_etfs = int(config.get("max_etfs_in_telegram", 9999))

    total_buy = 0
    total_sell = 0
    for changes in changes_by_etf.values():
        total_buy += len([item for item in changes if item.weight_delta > 0])
        total_sell += len([item for item in changes if item.weight_delta < 0])

    lines = [
        f"[KRX 액티브 ETF PDF 변화] {pretty_date}",
        f"대상 ETF: {len(etfs)}개 / 변화 ETF: {len(changed_etfs)}개",
        f"매수/비중증가: {total_buy}건 / 매도/비중감소: {total_sell}건",
        f"기준: 비중 변화 {config.get('min_weight_delta_pp', 0.05)}pp 이상, 신규/제외 종목 포함",
        f"표시: ETF별 매수 최대 {buy_limit}개, 매도 최대 {sell_limit}개",
    ]

    if skipped:
        lines.append(f"수집 실패/데이터 없음: {len(skipped)}개")
        for item in skipped[:10]:
            lines.append(f"- {item}")
        if len(skipped) > 10:
            lines.append(f"...외 {len(skipped) - 10}개")

    for etf in changed_etfs[:max_etfs]:
        changes = changes_by_etf[etf.ticker]
        buys = sorted([item for item in changes if item.weight_delta > 0], key=lambda item: item.weight_delta, reverse=True)
        sells = sorted([item for item in changes if item.weight_delta < 0], key=lambda item: item.weight_delta)

        lines.append("")
        lines.append(f"{etf.name} ({etf.ticker})")
        lines.append(f"매수/증가 {len(buys)}건, 매도/감소 {len(sells)}건")

        if buys:
            lines.append(f"[매수/비중증가 상위 {min(len(buys), buy_limit)}개]")
            for item in buys[:buy_limit]:
                label = "신규" if item.change_type == "ADDED" else "증가"
                lines.append(
                    f"- {label} {item.holding_name}({item.holding_code}) "
                    f"{format_weight(item.previous_weight)} -> {format_weight(item.current_weight)} "
                    f"({format_delta(item.weight_delta)})"
                )
        else:
            lines.append("[매수/비중증가 없음]")

        if sells:
            lines.append(f"[매도/비중감소 상위 {min(len(sells), sell_limit)}개]")
            for item in sells[:sell_limit]:
                label = "제외" if item.change_type == "REMOVED" else "감소"
                lines.append(
                    f"- {label} {item.holding_name}({item.holding_code}) "
                    f"{format_weight(item.previous_weight)} -> {format_weight(item.current_weight)} "
                    f"({format_delta(item.weight_delta)})"
                )
        else:
            lines.append("[매도/비중감소 없음]")

        hidden_buy = max(0, len(buys) - buy_limit)
        hidden_sell = max(0, len(sells) - sell_limit)
        if hidden_buy or hidden_sell:
            lines.append(f"...추가 변화: 매수 {hidden_buy}건, 매도 {hidden_sell}건은 DB에 저장됨")

    if len(changed_etfs) > max_etfs:
        lines.append(f"\n...외 {len(changed_etfs) - max_etfs}개 ETF 변화는 DB에 저장됨")
    if not changed_etfs:
        lines.append("\n감지된 비중 변화가 없습니다.")
    return "\n".join(lines)

def html_cell(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def build_html_report(
    trade_date: str,
    etfs: list[Etf],
    changes_by_etf: dict[str, list[Change]],
    skipped: list[str],
    config: dict[str, Any],
    run_id: int,
) -> Path:
    pretty_date = datetime.strptime(trade_date, "%Y%m%d").strftime("%Y-%m-%d")
    changed_etfs = [etf for etf in etfs if changes_by_etf.get(etf.ticker)]
    total_changes = sum(len(changes) for changes in changes_by_etf.values())
    total_buy = sum(len([item for item in changes if item.weight_delta > 0]) for changes in changes_by_etf.values())
    total_sell = sum(len([item for item in changes if item.weight_delta < 0]) for changes in changes_by_etf.values())

    latest_path = Path(str(config.get("html_report_path", "reports/latest_changes.html")))
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    dated_path = latest_path.parent / f"changes_{trade_date}_run_{run_id}.html"

    sections: list[str] = []
    for etf in changed_etfs:
        changes = changes_by_etf[etf.ticker]
        buys = sorted([item for item in changes if item.weight_delta > 0], key=lambda item: item.weight_delta, reverse=True)
        sells = sorted([item for item in changes if item.weight_delta < 0], key=lambda item: item.weight_delta)
        search_text = html_cell(f"{etf.name} {etf.ticker} " + " ".join([item.holding_name + ' ' + item.holding_code for item in changes]))

        def render_rows(items: list[Change], side: str) -> str:
            rows: list[str] = []
            for item in items:
                label = "신규" if item.change_type == "ADDED" else "제외" if item.change_type == "REMOVED" else ("증가" if item.weight_delta > 0 else "감소")
                rows.append(
                    "<tr>"
                    f"<td class=\"type {side}\">{label}</td>"
                    f"<td>{html_cell(item.holding_name)}</td>"
                    f"<td>{html_cell(item.holding_code)}</td>"
                    f"<td class=\"num\">{format_weight(item.previous_weight)}</td>"
                    f"<td class=\"num\">{format_weight(item.current_weight)}</td>"
                    f"<td class=\"num delta {side}\">{format_delta(item.weight_delta)}</td>"
                    "</tr>"
                )
            return "\n".join(rows) if rows else "<tr><td colspan=\"6\" class=\"empty\">해당 변화 없음</td></tr>"

        sections.append(
            f"""
            <section class="etf-card" data-search="{search_text}">
              <div class="etf-head">
                <div>
                  <h2>{html_cell(etf.name)} <span>{html_cell(etf.ticker)}</span></h2>
                  <p>매수/비중증가 {len(buys)}건 · 매도/비중감소 {len(sells)}건 · 전체 {len(changes)}건</p>
                </div>
              </div>
              <div class="tables">
                <div>
                  <h3>매수/비중증가 전체</h3>
                  <table>
                    <thead><tr><th>구분</th><th>종목명</th><th>코드</th><th>이전</th><th>현재</th><th>변화</th></tr></thead>
                    <tbody>{render_rows(buys, 'buy')}</tbody>
                  </table>
                </div>
                <div>
                  <h3>매도/비중감소 전체</h3>
                  <table>
                    <thead><tr><th>구분</th><th>종목명</th><th>코드</th><th>이전</th><th>현재</th><th>변화</th></tr></thead>
                    <tbody>{render_rows(sells, 'sell')}</tbody>
                  </table>
                </div>
              </div>
            </section>
            """
        )

    skipped_html = ""
    if skipped:
        skipped_items = "".join(f"<li>{html_cell(item)}</li>" for item in skipped)
        skipped_html = f"<details class=\"skipped\"><summary>수집 실패/데이터 없음 {len(skipped)}개</summary><ul>{skipped_items}</ul></details>"

    no_change_html = "" if changed_etfs else "<section class=\"etf-card\"><h2>감지된 비중 변화가 없습니다.</h2></section>"
    generated_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    body = "\n".join(sections)
    document = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KRX 액티브 ETF PDF 변화 {pretty_date}</title>
  <style>
    body {{ margin: 0; font-family: Arial, 'Malgun Gothic', sans-serif; background: #f6f7f9; color: #17202a; }}
    header {{ background: #0f766e; color: white; padding: 24px 28px; }}
    header h1 {{ margin: 0 0 8px; font-size: 24px; }}
    header p {{ margin: 0; opacity: .92; }}
    main {{ padding: 20px 28px 48px; }}
    .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 16px; }}
    .summary div {{ background: white; border: 1px solid #d8dee4; border-radius: 8px; padding: 14px; }}
    .summary b {{ display: block; font-size: 22px; margin-top: 4px; }}
    .tools {{ display: flex; gap: 10px; align-items: center; margin: 16px 0; }}
    input {{ width: min(520px, 100%); padding: 11px 12px; border: 1px solid #cbd5df; border-radius: 6px; font-size: 15px; }}
    .etf-card {{ background: white; border: 1px solid #d8dee4; border-radius: 8px; padding: 16px; margin: 14px 0; }}
    .etf-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: start; }}
    h2 {{ margin: 0; font-size: 19px; }}
    h2 span {{ color: #64748b; font-weight: 500; }}
    h3 {{ margin: 16px 0 8px; font-size: 15px; }}
    p {{ margin: 6px 0 0; color: #526173; }}
    .tables {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 16px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e7ebef; padding: 8px 7px; text-align: left; }}
    th {{ background: #f1f5f9; color: #334155; position: sticky; top: 0; }}
    .num {{ text-align: right; white-space: nowrap; }}
    .type {{ font-weight: 700; white-space: nowrap; }}
    .buy {{ color: #0f766e; }}
    .sell {{ color: #be123c; }}
    .empty {{ color: #64748b; text-align: center; }}
    .skipped {{ background: #fff7ed; border: 1px solid #fed7aa; border-radius: 8px; padding: 12px 14px; margin-bottom: 14px; }}
    .muted {{ color: #64748b; font-size: 13px; }}
    @media (max-width: 720px) {{ main {{ padding: 14px; }} .tables {{ grid-template-columns: 1fr; overflow-x: auto; }} table {{ min-width: 620px; }} }}
  </style>
</head>
<body>
<header>
  <h1>KRX 액티브 ETF PDF 변화 전체 리포트</h1>
  <p>{pretty_date} 기준 · 생성 {generated_at}</p>
</header>
<main>
  <div class="summary">
    <div>대상 ETF<b>{len(etfs)}</b></div>
    <div>변화 ETF<b>{len(changed_etfs)}</b></div>
    <div>전체 변화<b>{total_changes}</b></div>
    <div>매수/증가<b>{total_buy}</b></div>
    <div>매도/감소<b>{total_sell}</b></div>
  </div>
  <div class="tools"><input id="search" placeholder="ETF명, ETF코드, 종목명, 종목코드 검색"></div>
  <p class="muted">텔레그램은 요약만 보내고, 이 HTML에는 감지된 모든 변화가 표시됩니다.</p>
  {skipped_html}
  {no_change_html}
  {body}
</main>
<script>
  const input = document.getElementById('search');
  input.addEventListener('input', () => {{
    const q = input.value.trim().toLowerCase();
    document.querySelectorAll('.etf-card[data-search]').forEach(card => {{
      card.style.display = card.dataset.search.toLowerCase().includes(q) ? '' : 'none';
    }});
  }});
</script>
</body>
</html>"""

    latest_path.write_text(document, encoding="utf-8")
    dated_path.write_text(document, encoding="utf-8")
    return latest_path

def split_telegram_message(text: str, limit: int = 3900) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in text.splitlines():
        line_len = len(line) + 1
        if current and current_len + line_len > limit:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += line_len
    if current:
        chunks.append("\n".join(current))
    return chunks


def send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing in .env")
    for chunk in split_telegram_message(text):
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True},
            timeout=20,
        )
        response.raise_for_status()


def run_collection(args: argparse.Namespace) -> int:
    load_env_file()
    config = load_config(args.config)
    client = KrxClient()
    requested_date = normalize_date(args.date) if args.date else None
    trade_date, etfs = client.discover_active_etfs(config, requested_date)
    if not etfs:
        raise RuntimeError("No active ETFs found.")

    with open_db(config["database_path"]) as conn:
        init_db(conn)
        run_id = start_run(conn, trade_date)
        all_changes: list[Change] = []
        skipped: list[str] = []
        try:
            upsert_etfs(conn, etfs, trade_date)
            for etf in etfs:
                try:
                    actual_date, holdings = client.fetch_holdings(etf, requested_date)
                    time.sleep(float(args.sleep))
                    if not holdings:
                        skipped.append(f"{etf.name}({etf.ticker})")
                        continue
                    replace_holdings(conn, etf, actual_date, holdings)
                    prev_date = previous_trade_date_in_db(conn, etf.ticker, actual_date)
                    if not prev_date:
                        continue
                    changes = compare_holdings(
                        etf,
                        actual_date,
                        load_holdings_from_db(conn, etf.ticker, prev_date),
                        load_holdings_from_db(conn, etf.ticker, actual_date),
                        min_delta=float(config.get("min_weight_delta_pp", 0.05)),
                    )
                    all_changes.extend(changes)
                except Exception as exc:
                    skipped.append(f"{etf.name}({etf.ticker}): {exc}")
            save_changes(conn, run_id, all_changes)
            changes_by_etf: dict[str, list[Change]] = {}
            for change in all_changes:
                changes_by_etf.setdefault(change.etf_ticker, []).append(change)
            report = build_report(trade_date, etfs, changes_by_etf, skipped, config)
            html_path = build_html_report(trade_date, etfs, changes_by_etf, skipped, config, run_id)
            report = f"{report}\n\n전체 HTML 리포트: {html_path.resolve()}"
            finish_run(conn, run_id, "SUCCESS", f"{len(all_changes)} changes; html={html_path}")
        except Exception as exc:
            finish_run(conn, run_id, "FAILED", str(exc))
            raise

    print(report)
    if args.send_telegram:
        send_telegram(report)
    return 0


def run_discover(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    client = KrxClient()
    trade_date, etfs = client.discover_active_etfs(config, normalize_date(args.date) if args.date else None)
    for etf in etfs:
        print(f"{etf.ticker}\t{etf.isin}\t{etf.name}")
    print(f"\n{trade_date} 기준 {len(etfs)}개")
    return 0


def run_init_db(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    with open_db(config["database_path"]) as conn:
        init_db(conn)
    print(f"DB initialized: {config['database_path']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KRX active ETF PDF holdings monitor")
    parser.add_argument("--config", default="config.json", help="config JSON path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup_parser = subparsers.add_parser("setup-krx-login", help="save KRX login to Windows Credential Manager")
    setup_parser.set_defaults(func=lambda args: setup_krx_login())

    run_parser = subparsers.add_parser("run", help="collect holdings, save DB, and build report")
    run_parser.add_argument("--date", help="YYYYMMDD or YYYY-MM-DD. omitted means today/latest trading date")
    run_parser.add_argument("--send-telegram", action="store_true", help="send report to Telegram")
    run_parser.add_argument("--sleep", type=float, default=0.2, help="seconds to wait between ETFs")
    run_parser.set_defaults(func=run_collection)

    discover_parser = subparsers.add_parser("discover", help="print active ETF list")
    discover_parser.add_argument("--date", help="YYYYMMDD or YYYY-MM-DD. omitted means today/latest trading date")
    discover_parser.set_defaults(func=run_discover)

    init_parser = subparsers.add_parser("init-db", help="create SQLite schema")
    init_parser.set_defaults(func=run_init_db)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())






