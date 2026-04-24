from __future__ import annotations

import argparse
import getpass
import html
import json
import os
import sqlite3
import sys
import time
import textwrap
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
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

DEFAULT_CONFIG: dict[str, Any] = {
    "database_path": "data/krx_active_etf_holdings.sqlite",
    "watchlist": [],
    "include_keywords": ["액티브"],
    "exclude_keywords": [
        "금리", "국채", "국고채", "국공채", "금융채", "은행채", "회사채", "특수채", "전단채", "채권",
        "머니마켓", "유니콘", "hk", "ETF", "unicorn", "tdf",
    ],
    "min_weight_delta_pp": 0.05,
    "max_etfs_in_telegram": 9999,
    "max_changes_per_etf": 10,
    "max_buy_per_etf": 10,
    "max_sell_per_etf": 10,
    "html_report_path": "reports/latest_changes.html",
    "image_report_path": "reports/latest_changes.png",
    "public_report_url": "https://se2in.github.io/ETF_KRX/",
    "public_image_url": "https://se2in.github.io/ETF_KRX/latest_changes.png",
    "skip_unready_pdf": True,
    "unready_cash_weight_threshold": 90.0,
    "unready_removed_ratio_threshold": 0.6,
    "unready_missing_weight_ratio_threshold": 0.5,
    "unready_added_ratio_threshold": 0.1,
    "unready_current_ratio_threshold": 0.7,
}


@dataclass(frozen=True)
class Etf:
    ticker: str
    name: str
    isin: str = ""
    market_cap: float = 0.0


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
    previous_amount: float | None
    current_amount: float | None
    amount_delta: float
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
            previous_amount REAL,
            current_amount REAL,
            amount_delta REAL,
            change_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (run_id, etf_ticker, holding_code),
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS new_entries (
            run_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            etf_name TEXT NOT NULL,
            holding_code TEXT NOT NULL,
            holding_name TEXT NOT NULL,
            current_weight REAL NOT NULL,
            current_amount REAL,
            amount_delta REAL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (run_id, etf_ticker, holding_code),
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS daily_new_entries (
            trade_date TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            etf_name TEXT NOT NULL,
            holding_code TEXT NOT NULL,
            holding_name TEXT NOT NULL,
            current_weight REAL NOT NULL,
            current_amount REAL,
            amount_delta REAL,
            source_run_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (trade_date, etf_ticker, holding_code),
            FOREIGN KEY (source_run_id) REFERENCES runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS removed_entries (
            run_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            etf_name TEXT NOT NULL,
            holding_code TEXT NOT NULL,
            holding_name TEXT NOT NULL,
            previous_weight REAL,
            current_weight REAL,
            previous_amount REAL,
            current_amount REAL,
            amount_delta REAL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (run_id, etf_ticker, holding_code),
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS daily_removed_entries (
            trade_date TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            etf_name TEXT NOT NULL,
            holding_code TEXT NOT NULL,
            holding_name TEXT NOT NULL,
            previous_weight REAL,
            current_weight REAL,
            previous_amount REAL,
            current_amount REAL,
            amount_delta REAL,
            source_run_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (trade_date, etf_ticker, holding_code),
            FOREIGN KEY (source_run_id) REFERENCES runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS above_average_changes (
            run_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            side TEXT NOT NULL,
            etf_ticker TEXT NOT NULL,
            etf_name TEXT NOT NULL,
            holding_code TEXT NOT NULL,
            holding_name TEXT NOT NULL,
            previous_weight REAL,
            current_weight REAL,
            weight_delta REAL NOT NULL,
            previous_amount REAL,
            current_amount REAL,
            amount_delta REAL,
            average_abs_weight_delta REAL NOT NULL,
            change_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (run_id, side, etf_ticker, holding_code),
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        );
        """
    )
    ensure_column(conn, "etfs", "isin", "TEXT")
    ensure_column(conn, "changes", "previous_amount", "REAL")
    ensure_column(conn, "changes", "current_amount", "REAL")
    ensure_column(conn, "changes", "amount_delta", "REAL")
    conn.commit()


class KrxClient:
    def __init__(self) -> None:
        self.stock = get_pykrx_stock()
        from pykrx.website.krx import get_etx_isin
        from pykrx.website.krx.etx.core import PDF

        self.get_etx_isin = get_etx_isin
        self.pdf = PDF()

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
            isin = str(self.stock.get_etf_isin(ticker))
            if isin:
                return isin
        except Exception:
            pass
        try:
            return str(self.get_etx_isin(ticker))
        except Exception:
            return ""

    def etf_market_caps(self, trade_date: str) -> dict[str, float]:
        try:
            import pykrx.website.krx.etx.core as etx_core

            market_class = getattr(etx_core, "전종목시세_ETF")
            df = market_class().fetch(trade_date)
        except Exception:
            return {}

        caps: dict[str, float] = {}
        for row in df.to_dict(orient="records"):
            ticker = str(row.get("ISU_SRT_CD") or "").strip().zfill(6)
            if ticker:
                caps[ticker] = parse_number(row.get("MKTCAP")) or 0.0
        return caps


    def discover_active_etfs(self, config: dict[str, Any], date: str | None) -> tuple[str, list[Etf]]:
        trade_date = self.nearest_trade_date(date)
        watchlist = {str(ticker).zfill(6) for ticker in config.get("watchlist", []) if str(ticker).strip()}
        include_keywords = [str(v) for v in config.get("include_keywords", ["액티브"]) if str(v)]
        exclude_keywords = [str(v) for v in config.get("exclude_keywords", []) if str(v)]
        include_keywords_lower = [keyword.lower() for keyword in include_keywords]
        exclude_keywords_lower = [keyword.lower() for keyword in exclude_keywords]
        if not include_keywords and not watchlist:
            include_keywords = ["액티브"]

        market_caps = self.etf_market_caps(trade_date)
        etfs: list[Etf] = []
        for ticker in self.stock.get_etf_ticker_list(trade_date):
            ticker = str(ticker).zfill(6)
            name = self.etf_name(ticker)
            if watchlist and ticker not in watchlist:
                continue
            name_lower = name.lower()
            if not watchlist and include_keywords_lower and not any(keyword in name_lower for keyword in include_keywords_lower):
                continue
            if exclude_keywords_lower and any(keyword in name_lower for keyword in exclude_keywords_lower):
                continue
            etfs.append(Etf(ticker=ticker, name=name, isin=self.etf_isin(ticker), market_cap=market_caps.get(ticker, 0.0)))
        return trade_date, sorted(etfs, key=lambda item: (item.name, item.ticker))

    def fetch_holdings(self, etf: Etf, date: str | None) -> tuple[str, list[Holding]]:
        trade_date = self.nearest_trade_date(date)
        isin = etf.isin or self.etf_isin(etf.ticker)
        if not isin:
            return trade_date, []

        # pykrx's public helper currently prints noisy errors for KRX rows with no
        # PDF output. Reading the raw KRX PDF endpoint lets us handle empty tables
        # quietly and parse both old and new column layouts.
        df = self.pdf.fetch(trade_date, isin)
        if df is None or df.empty:
            return trade_date, []

        frame = df.reset_index() if getattr(df, "index", None) is not None and df.index.name else df.copy()
        records = frame.to_dict(orient="records")
        columns = [str(col) for col in frame.columns]
        rows: list[Holding] = []
        seen_codes: set[str] = set()
        for idx, row in enumerate(records, start=1):
            raw = {str(k): v for k, v in row.items()}
            values_by_pos = [raw.get(col) for col in columns]
            code = first_value(raw, ["\ud2f0\ucee4", "\uc885\ubaa9\ucf54\ub4dc", "COMPST_ISU_CD", "COMPST_ISU_CD2", "ISU_CD", "index"])
            name = first_value(raw, ["\uad6c\uc131\uc885\ubaa9\uba85", "\uc885\ubaa9\uba85", "COMPST_ISU_NM", "ISU_NM", "KOR_ISU_NM"])
            quantity = first_value(raw, ["\uacc4\uc57d\uc218", "\uc218\ub7c9", "COMPST_ISU_CU1_SHRS", "CU1_SHRS", "SHRS"])
            amount = first_value(raw, ["\uae08\uc561", "\ud3c9\uac00\uae08\uc561", "VALU_AMT", "EVAL_AMT"])
            weight = first_value(raw, ["\ube44\uc911", "\ube44\uc911(%)", "COMPST_RTO", "RTO", "WEIGHT"])

            if code is None and values_by_pos:
                code = values_by_pos[0]
            if name is None:
                for value in values_by_pos:
                    text = str(value or "").strip()
                    if text and not text.startswith("KR") and not text.replace(",", "").replace(".", "").replace("-", "").isdigit():
                        name = value
                        break
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
        "SELECT holding_code, holding_name, amount, weight FROM holdings WHERE etf_ticker = ? AND trade_date = ?",
        (etf_ticker, trade_date),
    ).fetchall()
    return {row["holding_code"]: row for row in rows}


def delete_holdings_for_date(conn: sqlite3.Connection, etf: Etf, trade_date: str) -> None:
    conn.execute("DELETE FROM holdings WHERE trade_date = ? AND etf_ticker = ?", (trade_date, etf.ticker))
    conn.commit()


def holding_compare_rows(holdings: list[Holding]) -> dict[str, dict[str, Any]]:
    return {item.holding_code: {"holding_name": item.holding_name, "amount": item.amount, "weight": item.weight} for item in holdings}


def row_holding_name(row: Any) -> str:
    try:
        return str(row["holding_name"] or "")
    except Exception:
        return ""


def row_weight(row: Any) -> float | None:
    try:
        value = row["weight"]
    except Exception:
        return None
    return None if value is None else float(value)


def row_amount(row: Any) -> float | None:
    try:
        value = row["amount"]
    except Exception:
        return None
    return None if value is None else float(value)


def is_cash_holding(code: str, row: Any) -> bool:
    name = row_holding_name(row)
    return code in {"010010", "CASH", "KRW"} or "\ud604\uae08" in name or "CASH" in name.upper()


def unready_pdf_reason(
    etf: Etf,
    previous: dict[str, Any],
    current: dict[str, Any],
    config: dict[str, Any],
) -> str | None:
    if not config.get("skip_unready_pdf", True):
        return None
    if len(previous) < 5 or not current:
        return None

    previous_codes = set(previous)
    current_codes = set(current)
    removed_ratio = len(previous_codes - current_codes) / max(1, len(previous_codes))
    added_ratio = len(current_codes - previous_codes) / max(1, len(previous_codes))
    current_ratio = len(current_codes) / max(1, len(previous_codes))
    missing_weight_ratio = len([row for row in current.values() if row_weight(row) is None]) / max(1, len(current_codes))
    cash_weight = sum((row_weight(row) or 0.0) for code, row in current.items() if is_cash_holding(code, row))

    cash_threshold = float(config.get("unready_cash_weight_threshold", 90.0))
    removed_threshold = float(config.get("unready_removed_ratio_threshold", 0.6))
    missing_weight_threshold = float(config.get("unready_missing_weight_ratio_threshold", 0.5))
    added_threshold = float(config.get("unready_added_ratio_threshold", 0.1))
    current_ratio_threshold = float(config.get("unready_current_ratio_threshold", 0.7))

    if missing_weight_ratio >= missing_weight_threshold:
        return f"PDF \ubbf8\uc5c5\ub370\uc774\ud2b8 \uc758\uc2ec: \ud604\uc7ac PDF \ube44\uc911 \uac12 {missing_weight_ratio:.0%} \ub204\ub77d"
    if cash_weight >= cash_threshold and removed_ratio >= removed_threshold and current_ratio <= 0.5:
        return f"PDF \ubbf8\uc5c5\ub370\uc774\ud2b8 \uc758\uc2ec: \uc6d0\ud654\ud604\uae08 {cash_weight:.2f}%, \uae30\uc874 \uc885\ubaa9 {removed_ratio:.0%} \ub204\ub77d"
    if removed_ratio >= removed_threshold and added_ratio <= added_threshold and current_ratio <= current_ratio_threshold:
        return f"PDF \ubbf8\uc5c5\ub370\uc774\ud2b8 \uc758\uc2ec: \uae30\uc874 \uc885\ubaa9 {removed_ratio:.0%} \ub204\ub77d, \uc2e0\uaddc \uc885\ubaa9 {added_ratio:.0%}"
    return None


def compare_holdings(etf: Etf, trade_date: str, previous: dict[str, Any], current: dict[str, Any], min_delta: float) -> list[Change]:
    changes: list[Change] = []
    for code in set(previous) | set(current):
        prev = previous.get(code)
        curr = current.get(code)
        prev_weight_raw = row_weight(prev) if prev else None
        curr_weight_raw = row_weight(curr) if curr else None
        prev_amount_raw = row_amount(prev) if prev else None
        curr_amount_raw = row_amount(curr) if curr else None

        if prev and curr and (prev_weight_raw is None or curr_weight_raw is None):
            continue

        prev_weight = 0.0 if prev is None else prev_weight_raw
        curr_weight = 0.0 if curr is None else curr_weight_raw
        prev_amount = 0.0 if prev is None else prev_amount_raw
        curr_amount = 0.0 if curr is None else curr_amount_raw
        delta = round(float(curr_weight or 0.0) - float(prev_weight or 0.0), 6)
        amount_delta = round(float(curr_amount or 0.0) - float(prev_amount or 0.0), 2)

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
                previous_amount=prev_amount,
                current_amount=curr_amount,
                amount_delta=amount_delta,
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
            previous_weight, current_weight, weight_delta,
            previous_amount, current_amount, amount_delta,
            change_type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                item.previous_amount,
                item.current_amount,
                item.amount_delta,
                item.change_type,
                now,
            )
            for item in changes
        ],
    )
    conn.commit()


def is_new_entry(change: Change) -> bool:
    return (
        change.current_weight is not None
        and float(change.current_weight) > 0
        and float(change.previous_weight or 0.0) <= 0
        and change.weight_delta > 0
    )


def is_removed_entry(change: Change) -> bool:
    return change.change_type == "REMOVED" or (
        float(change.previous_weight or 0.0) > 0
        and float(change.current_weight or 0.0) <= 0
        and change.weight_delta < 0
    )


def is_cash_change(change: Change) -> bool:
    return (change.holding_code or "") == "010010" or "원화현금" in (change.holding_name or "")


def above_average_change_sets(changes: list[Change]) -> tuple[float, float, list[Change], list[Change]]:
    buy_changes = [item for item in changes if item.weight_delta > 0]
    sell_changes = [item for item in changes if item.weight_delta < 0]
    buy_average = sum(item.weight_delta for item in buy_changes) / len(buy_changes) if buy_changes else 0.0
    sell_average = sum(abs(item.weight_delta) for item in sell_changes) / len(sell_changes) if sell_changes else 0.0
    above_average_buys = sorted(
        [item for item in buy_changes if item.weight_delta >= buy_average],
        key=lambda item: item.weight_delta,
        reverse=True,
    )
    above_average_sells = sorted(
        [item for item in sell_changes if abs(item.weight_delta) >= sell_average],
        key=lambda item: abs(item.weight_delta),
        reverse=True,
    )
    return buy_average, sell_average, above_average_buys, above_average_sells


def save_new_entries(conn: sqlite3.Connection, run_id: int, changes: list[Change]) -> None:
    entries = [item for item in changes if is_new_entry(item)]
    if not entries:
        return
    now = datetime.now(KST).isoformat(timespec="seconds")
    conn.executemany(
        """
        INSERT OR REPLACE INTO new_entries (
            run_id, trade_date, etf_ticker, etf_name,
            holding_code, holding_name, current_weight,
            current_amount, amount_delta, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                item.trade_date,
                item.etf_ticker,
                item.etf_name,
                item.holding_code,
                item.holding_name,
                item.current_weight,
                item.current_amount,
                item.amount_delta,
                now,
            )
            for item in entries
        ],
    )
    conn.commit()


def save_daily_new_entries(conn: sqlite3.Connection, run_id: int, trade_date: str, changes: list[Change]) -> None:
    entries = [item for item in changes if is_new_entry(item)]
    now = datetime.now(KST).isoformat(timespec="seconds")
    conn.execute("DELETE FROM daily_new_entries WHERE trade_date = ?", (trade_date,))
    if entries:
        conn.executemany(
            """
            INSERT OR REPLACE INTO daily_new_entries (
                trade_date, etf_ticker, etf_name, holding_code, holding_name,
                current_weight, current_amount, amount_delta, source_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item.trade_date,
                    item.etf_ticker,
                    item.etf_name,
                    item.holding_code,
                    item.holding_name,
                    item.current_weight,
                    item.current_amount,
                    item.amount_delta,
                    run_id,
                    now,
                )
                for item in entries
            ],
        )
    conn.commit()


def save_removed_entries(conn: sqlite3.Connection, run_id: int, changes: list[Change]) -> None:
    entries = [item for item in changes if is_removed_entry(item) and not is_cash_change(item)]
    if not entries:
        return
    now = datetime.now(KST).isoformat(timespec="seconds")
    conn.executemany(
        """
        INSERT OR REPLACE INTO removed_entries (
            run_id, trade_date, etf_ticker, etf_name,
            holding_code, holding_name, previous_weight, current_weight,
            previous_amount, current_amount, amount_delta, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                item.trade_date,
                item.etf_ticker,
                item.etf_name,
                item.holding_code,
                item.holding_name,
                item.previous_weight,
                item.current_weight,
                item.previous_amount,
                item.current_amount,
                item.amount_delta,
                now,
            )
            for item in entries
        ],
    )
    conn.commit()


def save_daily_removed_entries(conn: sqlite3.Connection, run_id: int, trade_date: str, changes: list[Change]) -> None:
    entries = [item for item in changes if is_removed_entry(item) and not is_cash_change(item)]
    now = datetime.now(KST).isoformat(timespec="seconds")
    conn.execute("DELETE FROM daily_removed_entries WHERE trade_date = ?", (trade_date,))
    if entries:
        conn.executemany(
            """
            INSERT OR REPLACE INTO daily_removed_entries (
                trade_date, etf_ticker, etf_name, holding_code, holding_name,
                previous_weight, current_weight, previous_amount, current_amount,
                amount_delta, source_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    item.trade_date,
                    item.etf_ticker,
                    item.etf_name,
                    item.holding_code,
                    item.holding_name,
                    item.previous_weight,
                    item.current_weight,
                    item.previous_amount,
                    item.current_amount,
                    item.amount_delta,
                    run_id,
                    now,
                )
                for item in entries
            ],
        )
    conn.commit()


def save_above_average_changes(conn: sqlite3.Connection, run_id: int, changes: list[Change]) -> None:
    buy_average, sell_average, above_average_buys, above_average_sells = above_average_change_sets(changes)
    rows: list[tuple[str, float, Change]] = [
        *[("BUY", buy_average, item) for item in above_average_buys],
        *[("SELL", sell_average, item) for item in above_average_sells],
    ]
    if not rows:
        return
    now = datetime.now(KST).isoformat(timespec="seconds")
    conn.executemany(
        """
        INSERT OR REPLACE INTO above_average_changes (
            run_id, trade_date, side, etf_ticker, etf_name,
            holding_code, holding_name, previous_weight, current_weight,
            weight_delta, previous_amount, current_amount, amount_delta,
            average_abs_weight_delta, change_type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                item.trade_date,
                side,
                item.etf_ticker,
                item.etf_name,
                item.holding_code,
                item.holding_name,
                item.previous_weight,
                item.current_weight,
                item.weight_delta,
                item.previous_amount,
                item.current_amount,
                item.amount_delta,
                average_abs_weight_delta,
                item.change_type,
                now,
            )
            for side, average_abs_weight_delta, item in rows
        ],
    )
    conn.commit()


def format_weight(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}%"


def format_delta(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}pp"


def format_krw(value: float | None) -> str:
    if value is None:
        return "-"
    amount = abs(float(value))
    if amount >= 100_000_000:
        return f"{amount / 100_000_000:.1f}\uc5b5\uc6d0"
    if amount >= 10_000:
        return f"{amount / 10_000:.0f}\ub9cc\uc6d0"
    return f"{amount:,.0f}\uc6d0"


def top_market_cap_etfs(etfs: list[Etf], limit: int = 10) -> list[Etf]:
    return sorted(etfs, key=lambda item: float(item.market_cap or 0.0), reverse=True)[:limit]


def top_new_entries(changes_by_etf: dict[str, list[Change]], limit: int = 10) -> list[Change]:
    entries = [item for changes in changes_by_etf.values() for item in changes if is_new_entry(item)]
    return sorted(
        entries,
        key=lambda item: (float(item.current_weight or 0.0), abs(float(item.amount_delta or 0.0))),
        reverse=True,
    )[:limit]


def is_large_new_entry(change: Change) -> bool:
    return is_new_entry(change) and abs(float(change.amount_delta or 0.0)) >= 10_000_000


def html_signal_class(change: Change, fallback: str) -> str:
    return "gold" if is_large_new_entry(change) else fallback


def html_signal_row_class(change: Change) -> str:
    return "gold-row" if is_large_new_entry(change) else ""


def build_report(trade_date: str, etfs: list[Etf], changes_by_etf: dict[str, list[Change]], skipped: list[str], config: dict[str, Any]) -> str:
    generated_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    top_etfs = top_market_cap_etfs(etfs, 10)
    new_entry_top10 = top_new_entries(changes_by_etf, 10)
    separator = "-" * 91

    lines = [
        f"\uc720\uc9c4\uc99d\uad8c \uc548\uc0c1\ud604 \uc13c\ud130\uc7a5\uc758 ETF \ubaa8\ub2c8\ud130\ub9c1 ({generated_at})",
        "",
        separator,
        "",
        "\uc2e0\uaddc \ud3b8\uc785 \uc885\ubaa9 \uc0c1\uc704 10\uac1c",
    ]

    if new_entry_top10:
        for rank, item in enumerate(new_entry_top10, start=1):
            lines.append(
                f"{rank}. 🟢 {item.holding_name}({item.holding_code}) / "
                f"{item.etf_name}({item.etf_ticker}) "
                f"{format_weight(item.previous_weight)} -> {format_weight(item.current_weight)} "
                f"({format_delta(item.weight_delta)}, {format_krw(item.amount_delta)})"
            )
    else:
        lines.append("\uc2e0\uaddc \ud3b8\uc785 \uc885\ubaa9\uc774 \uc5c6\uc2b5\ub2c8\ub2e4.")

    lines.extend(
        [
            "",
            separator,
            "",
        "\uc561\ud2f0\ube0c ETF\uc758 \uc2dc\uac00\ucd1d\uc561 \uc0c1\uc704 10\uac1c\uc758 \ub9e4\uc218/\ub9e4\ub3c4 \uc804\uc885\ubaa9",
        "",
        separator,
        ]
    )

    for rank, etf in enumerate(top_etfs, start=1):
        changes = changes_by_etf.get(etf.ticker, [])
        buys = sorted([item for item in changes if item.weight_delta > 0], key=lambda item: item.weight_delta, reverse=True)
        sells = sorted([item for item in changes if item.weight_delta < 0], key=lambda item: item.weight_delta)

        lines.append("")
        lines.append(f"{rank}. {etf.name} ({etf.ticker}) / \uc2dc\uac00\ucd1d\uc561 {format_krw(etf.market_cap)}")
        lines.append(f"\ub9e4\uc218/\uc99d\uac00 {len(buys)}\uac74, \ub9e4\ub3c4/\uac10\uc18c {len(sells)}\uac74")

        if buys:
            lines.append("[\ub9e4\uc218/\ube44\uc911\uc99d\uac00 \uc804\uc885\ubaa9]")
            for item in buys:
                marker = "🟢" if is_new_entry(item) else "🔴"
                label = "\uc2e0\uaddc" if is_new_entry(item) else "\uc99d\uac00"
                lines.append(
                    f"- {marker} {label} {item.holding_name}({item.holding_code}) "
                    f"{format_weight(item.previous_weight)} -> {format_weight(item.current_weight)} "
                    f"({format_delta(item.weight_delta)}, {format_krw(item.amount_delta)})"
                )
        else:
            lines.append("[\ub9e4\uc218/\ube44\uc911\uc99d\uac00 \uc5c6\uc74c]")

        if sells:
            lines.append("[\ub9e4\ub3c4/\ube44\uc911\uac10\uc18c \uc804\uc885\ubaa9]")
            for item in sells:
                label = "\uc81c\uc678" if item.change_type == "REMOVED" else "\uac10\uc18c"
                lines.append(
                    f"- 🔵 {label} {item.holding_name}({item.holding_code}) "
                    f"{format_weight(item.previous_weight)} -> {format_weight(item.current_weight)} "
                    f"({format_delta(item.weight_delta)}, {format_krw(item.amount_delta)})"
                )
        else:
            lines.append("[\ub9e4\ub3c4/\ube44\uc911\uac10\uc18c \uc5c6\uc74c]")

    if skipped:
        lines.append("")
        lines.append(separator)
        lines.append(f"PDF \ubbf8\uc5c5\ub370\uc774\ud2b8/\ub370\uc774\ud130 \uc5c6\uc74c: {len(skipped)}\uac1c")
        for item in skipped[:15]:
            lines.append(f"- {item}")
        if len(skipped) > 15:
            lines.append(f"...\uc678 {len(skipped) - 15}\uac1c")

    if not any(changes_by_etf.get(etf.ticker) for etf in top_etfs):
        lines.append("")
        lines.append("\uc2dc\uac00\ucd1d\uc561 \uc0c1\uc704 10\uac1c \uc561\ud2f0\ube0c ETF\uc5d0\uc11c \uac10\uc9c0\ub41c \ube44\uc911 \ubcc0\ud654\uac00 \uc5c6\uc2b5\ub2c8\ub2e4.")
    return "\n".join(lines)

def html_cell(value: Any) -> str:
    return html.escape("" if value is None else str(value))



def aggregate_amount_flows(changes_by_etf: dict[str, list[Change]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    buys: dict[str, dict[str, Any]] = {}
    sells: dict[str, dict[str, Any]] = {}
    for changes in changes_by_etf.values():
        for item in changes:
            if item.amount_delta == 0:
                continue
            target = buys if item.amount_delta > 0 else sells
            key = item.holding_code or item.holding_name
            row = target.setdefault(
                key,
                {"holding_code": item.holding_code, "holding_name": item.holding_name, "amount": 0.0, "etfs": set()},
            )
            row["amount"] += abs(float(item.amount_delta or 0.0))
            row["etfs"].add(item.etf_ticker)
    buy_rows = sorted(buys.values(), key=lambda row: row["amount"], reverse=True)[:20]
    sell_rows = sorted(sells.values(), key=lambda row: row["amount"], reverse=True)[:20]
    return buy_rows, sell_rows


def render_amount_flow_html(changes_by_etf: dict[str, list[Change]]) -> str:
    buy_rows, sell_rows = aggregate_amount_flows(changes_by_etf)
    all_changes = [item for changes in changes_by_etf.values() for item in changes]
    buy_average, sell_average, above_average_buys, above_average_sells = above_average_change_sets(all_changes)
    new_entries = sorted(
        [item for item in all_changes if is_new_entry(item)],
        key=lambda item: (float(item.current_weight or 0.0), abs(float(item.amount_delta or 0.0))),
        reverse=True,
    )
    removed_entries = sorted(
        [item for item in all_changes if is_removed_entry(item) and not is_cash_change(item)],
        key=lambda item: (float(item.previous_weight or 0.0), abs(float(item.amount_delta or 0.0))),
        reverse=True,
    )

    def render_rows(rows: list[dict[str, Any]], side: str) -> str:
        if not rows:
            return "<tr><td colspan=\"5\" class=\"empty\">\ud574\ub2f9 \ubcc0\ud654 \uc5c6\uc74c</td></tr>"
        html_rows = []
        for idx, row in enumerate(rows, start=1):
            html_rows.append(
                "<tr>"
                f"<td class=\"num\">{idx}</td>"
                f"<td>{html_cell(row['holding_name'])}</td>"
                f"<td>{html_cell(row['holding_code'])}</td>"
                f"<td class=\"num {side}\">{format_krw(row['amount'])}</td>"
                f"<td class=\"num\">{len(row['etfs'])}</td>"
                "</tr>"
            )
        return "\n".join(html_rows)

    def render_average_change_rows(items: list[Change], side: str) -> str:
        if not items:
            return "<tr><td colspan=\"9\" class=\"empty\">\ud3c9\uade0 \uc774\uc0c1 \ube44\uc911 \ubcc0\ud654 \uc885\ubaa9 \uc5c6\uc74c</td></tr>"
        html_rows = []
        for idx, item in enumerate(items, start=1):
            label = "\uc2e0\uaddc" if is_new_entry(item) else ("\uc99d\uac00" if item.weight_delta > 0 else "\uac10\uc18c")
            row_side = html_signal_class(item, "new" if is_new_entry(item) else side)
            html_rows.append(
                f"<tr class=\"{html_signal_row_class(item)}\">"
                f"<td class=\"num\">{idx}</td>"
                f"<td class=\"type {row_side}\">{label}</td>"
                f"<td>{html_cell(item.etf_name)}</td>"
                f"<td>{html_cell(item.etf_ticker)}</td>"
                f"<td>{html_cell(item.holding_name)}</td>"
                f"<td>{html_cell(item.holding_code)}</td>"
                f"<td class=\"num\">{format_weight(item.previous_weight)}</td>"
                f"<td class=\"num\">{format_weight(item.current_weight)}</td>"
                f"<td class=\"num {row_side}\">{format_delta(item.weight_delta)}</td>"
                "</tr>"
            )
        return "\n".join(html_rows)

    def render_new_entry_rows(items: list[Change]) -> str:
        if not items:
            return "<tr><td colspan=\"8\" class=\"empty\">\uc2e0\uaddc \ud3b8\uc785 \uc885\ubaa9 \uc5c6\uc74c</td></tr>"
        html_rows = []
        for idx, item in enumerate(items, start=1):
            signal_class = html_signal_class(item, "new")
            html_rows.append(
                f"<tr class=\"{html_signal_row_class(item)}\">"
                f"<td class=\"num\">{idx}</td>"
                f"<td>{html_cell(item.etf_name)}</td>"
                f"<td>{html_cell(item.etf_ticker)}</td>"
                f"<td>{html_cell(item.holding_name)}</td>"
                f"<td>{html_cell(item.holding_code)}</td>"
                f"<td class=\"num\">{format_weight(item.previous_weight)}</td>"
                f"<td class=\"num {signal_class}\">{format_weight(item.current_weight)}</td>"
                f"<td class=\"num {signal_class}\">{format_krw(item.amount_delta)}</td>"
                "</tr>"
            )
        return "\n".join(html_rows)

    def render_removed_entry_rows(items: list[Change]) -> str:
        if not items:
            return "<tr><td colspan=\"8\" class=\"empty\">\ud3b8\uc785 \uc81c\uc678 \uc885\ubaa9 \uc5c6\uc74c</td></tr>"
        html_rows = []
        for idx, item in enumerate(items, start=1):
            html_rows.append(
                "<tr>"
                f"<td class=\"num\">{idx}</td>"
                f"<td>{html_cell(item.etf_name)}</td>"
                f"<td>{html_cell(item.etf_ticker)}</td>"
                f"<td>{html_cell(item.holding_name)}</td>"
                f"<td>{html_cell(item.holding_code)}</td>"
                f"<td class=\"num sell\">{format_weight(item.previous_weight)}</td>"
                f"<td class=\"num sell\">{format_weight(item.current_weight)}</td>"
                f"<td class=\"num sell\">{format_krw(item.amount_delta)}</td>"
                "</tr>"
            )
        return "\n".join(html_rows)

    return f"""
    <section class="etf-card flow-board">
      <div class="etf-head">
        <div>
          <h2>\uc885\ubaa9\ubcc4 \ud569\uc0b0 \ub9e4\uc218/\ub9e4\ub3c4 \uae08\uc561 TOP 20</h2>
          <p>\uac01 ETF PDF\uc758 \ud3c9\uac00\uae08\uc561 \ubcc0\ud654\ub97c \uc885\ubaa9\ubcc4\ub85c \ud569\uc0b0\ud55c \uac12\uc785\ub2c8\ub2e4.</p>
        </div>
      </div>
      <div class="tables">
        <div>
          <h3>\ud569\uc0b0 \ub9e4\uc218 \uae08\uc561 TOP 20</h3>
          <table>
            <thead><tr><th>#</th><th>\uc885\ubaa9\uba85</th><th>\ucf54\ub4dc</th><th>\ud569\uc0b0\uae08\uc561</th><th>ETF\uc218</th></tr></thead>
            <tbody>{render_rows(buy_rows, 'buy')}</tbody>
          </table>
        </div>
        <div>
          <h3>\ud569\uc0b0 \ub9e4\ub3c4 \uae08\uc561 TOP 20</h3>
          <table>
            <thead><tr><th>#</th><th>\uc885\ubaa9\uba85</th><th>\ucf54\ub4dc</th><th>\ud569\uc0b0\uae08\uc561</th><th>ETF\uc218</th></tr></thead>
            <tbody>{render_rows(sell_rows, 'sell')}</tbody>
          </table>
        </div>
      </div>
      <div class="new-entry-board">
        <h3>\uc2e0\uaddc \ud3b8\uc785 \uc885\ubaa9 \uc804\uccb4</h3>
        <table>
          <thead><tr><th>#</th><th>ETF</th><th>ETF\ucf54\ub4dc</th><th>\uc885\ubaa9\uba85</th><th>\ucf54\ub4dc</th><th>\uc774\uc804</th><th>\ud604\uc7ac \ube44\uc911</th><th>\uae08\uc561\ubcc0\ud654</th></tr></thead>
          <tbody>{render_new_entry_rows(new_entries)}</tbody>
        </table>
      </div>
      <div class="removed-entry-board">
        <h3>\ud3b8\uc785 \uc81c\uc678 \uc885\ubaa9 \uc804\uccb4</h3>
        <table>
          <thead><tr><th>#</th><th>ETF</th><th>ETF\ucf54\ub4dc</th><th>\uc885\ubaa9\uba85</th><th>\ucf54\ub4dc</th><th>\uc774\uc804</th><th>\ud604\uc7ac \ube44\uc911</th><th>\uae08\uc561\ubcc0\ud654</th></tr></thead>
          <tbody>{render_removed_entry_rows(removed_entries)}</tbody>
        </table>
      </div>
      <div class="average-change-board">
        <h3>\ud3c9\uade0 \uc774\uc0c1 \ube44\uc911 \ubcc0\ud654 \uc885\ubaa9</h3>
        <p>\ub9e4\uc218 \ud3c9\uade0 {format_delta(buy_average)} \uc774\uc0c1, \ub9e4\ub3c4 \ud3c9\uade0 -{sell_average:.2f}pp \uc774\uc0c1\uc758 \ubcc0\ud654\ub9cc \ub530\ub85c \ubaa8\uc558\uc2b5\ub2c8\ub2e4.</p>
        <div class="tables">
          <div>
            <h3>\ub9e4\uc218/\uc99d\uac00 \ud3c9\uade0 \uc774\uc0c1</h3>
            <table>
              <thead><tr><th>#</th><th>\uad6c\ubd84</th><th>ETF</th><th>ETF\ucf54\ub4dc</th><th>\uc885\ubaa9\uba85</th><th>\ucf54\ub4dc</th><th>\uc774\uc804</th><th>\ud604\uc7ac</th><th>\ubcc0\ud654</th></tr></thead>
              <tbody>{render_average_change_rows(above_average_buys, 'buy')}</tbody>
            </table>
          </div>
          <div>
            <h3>\ub9e4\ub3c4/\uac10\uc18c \ud3c9\uade0 \uc774\uc0c1</h3>
            <table>
              <thead><tr><th>#</th><th>\uad6c\ubd84</th><th>ETF</th><th>ETF\ucf54\ub4dc</th><th>\uc885\ubaa9\uba85</th><th>\ucf54\ub4dc</th><th>\uc774\uc804</th><th>\ud604\uc7ac</th><th>\ubcc0\ud654</th></tr></thead>
              <tbody>{render_average_change_rows(above_average_sells, 'sell')}</tbody>
            </table>
          </div>
        </div>
      </div>
    </section>
    """


def build_image_report(
    trade_date: str,
    etfs: list[Etf],
    changes_by_etf: dict[str, list[Change]],
    skipped: list[str],
    config: dict[str, Any],
    run_id: int,
) -> Path:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:
        raise RuntimeError("Pillow is not installed. Run: python -m pip install -r requirements.txt") from exc

    latest_path = Path(str(config.get("image_report_path", "reports/latest_changes.png")))
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    dated_path = latest_path.parent / f"changes_{trade_date}_run_{run_id}.png"

    pretty_date = datetime.strptime(trade_date, "%Y%m%d").strftime("%Y-%m-%d")
    generated_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    changed_etfs = [etf for etf in etfs if changes_by_etf.get(etf.ticker)]
    all_changes = [item for changes in changes_by_etf.values() for item in changes]
    total_buy = len([item for item in all_changes if item.weight_delta > 0])
    total_sell = len([item for item in all_changes if item.weight_delta < 0])
    buy_average, sell_average, above_buys, above_sells = above_average_change_sets(all_changes)
    new_entries = top_new_entries(changes_by_etf, 10)
    amount_buys, amount_sells = aggregate_amount_flows(changes_by_etf)

    width, height = 1600, 2600
    image = Image.new("RGB", (width, height), "#050608")
    draw = ImageDraw.Draw(image)

    def load_font(size: int, bold: bool = False) -> Any:
        candidates = [
            Path("C:/Windows/Fonts/malgunbd.ttf" if bold else "C:/Windows/Fonts/malgun.ttf"),
            Path("C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"),
        ]
        for candidate in candidates:
            if candidate.exists():
                return ImageFont.truetype(str(candidate), size)
        return ImageFont.load_default()

    font_hero = load_font(54, True)
    font_h1 = load_font(34, True)
    font_h2 = load_font(25, True)
    font_body = load_font(22)
    font_small = load_font(18)
    font_num = load_font(24, True)

    bg_panel = "#101318"
    line = "#2a3038"
    amber = "#f5b301"
    text = "#f2f5f8"
    muted = "#9aa6b2"
    red = "#ff4d5e"
    cyan = "#3dd6e8"
    green = "#19c37d"
    gold = "#ffd24a"

    draw.rectangle([0, 0, width, 260], fill="#0b1016")
    draw.rectangle([0, 258, width, 260], fill=amber)
    draw.text((42, 32), "EUGENE SECURITIES | ACTIVE ETF MONITOR", font=font_small, fill=amber)
    draw.text((42, 78), "유진증권 안상현 센터장의 ETF 변동율 체크", font=font_hero, fill=text)
    draw.text((42, 156), f"{pretty_date} 기준 | 생성 {generated_at} | AI 이미지 리포트", font=font_body, fill=muted)

    metrics = [
        ("대상 ETF", len(etfs), amber),
        ("변화 ETF", len(changed_etfs), cyan),
        ("전체 변화", len(all_changes), text),
        ("매수/증가", total_buy, red),
        ("매도/감소", total_sell, cyan),
    ]
    x, y = 42, 300
    box_w, box_h, gap = 286, 110, 18
    for label, value, color in metrics:
        draw.rounded_rectangle([x, y, x + box_w, y + box_h], radius=8, fill=bg_panel, outline=line, width=2)
        draw.rectangle([x, y, x + 6, y + box_h], fill=amber)
        draw.text((x + 24, y + 18), label, font=font_small, fill=muted)
        draw.text((x + 24, y + 52), f"{value:,}", font=font_num, fill=color)
        x += box_w + gap

    def section_title(title: str, subtitle: str, top: int) -> int:
        draw.text((42, top), title, font=font_h1, fill=text)
        if subtitle:
            draw.text((42, top + 42), subtitle, font=font_small, fill=muted)
            return top + 82
        return top + 52

    def truncate(value: str, length: int) -> str:
        return value if len(value) <= length else value[: length - 1] + "…"

    def draw_change_rows(title: str, rows: list[Change], top: int, color: str, average_label: str = "") -> int:
        draw.rounded_rectangle([42, top, 1558, top + 360], radius=8, fill=bg_panel, outline=line, width=2)
        draw.text((66, top + 22), title, font=font_h2, fill=color)
        if average_label:
            draw.text((1180, top + 25), average_label, font=font_small, fill=muted)
        y_row = top + 72
        if not rows:
            draw.text((66, y_row), "해당 변화 없음", font=font_body, fill=muted)
            return top + 390
        for idx, item in enumerate(rows[:8], start=1):
            label = "신규" if is_new_entry(item) else ("증가" if item.weight_delta > 0 else "감소")
            line_text = (
                f"{idx}. {label} {truncate(item.holding_name, 18)}({item.holding_code}) | "
                f"{truncate(item.etf_name, 24)} | {format_weight(item.previous_weight)} -> "
                f"{format_weight(item.current_weight)} ({format_delta(item.weight_delta)})"
            )
            draw.text((66, y_row), line_text, font=font_body, fill=gold if is_large_new_entry(item) else text)
            y_row += 34
        return top + 390

    def draw_amount_rows(title: str, rows: list[dict[str, Any]], top: int, color: str) -> int:
        draw.rounded_rectangle([42, top, 1558, top + 280], radius=8, fill=bg_panel, outline=line, width=2)
        draw.text((66, top + 22), title, font=font_h2, fill=color)
        y_row = top + 72
        if not rows:
            draw.text((66, y_row), "해당 변화 없음", font=font_body, fill=muted)
            return top + 310
        for idx, row in enumerate(rows[:6], start=1):
            row_text = (
                f"{idx}. {truncate(str(row['holding_name']), 22)}({row['holding_code']}) | "
                f"{format_krw(row['amount'])} | ETF {len(row['etfs'])}개"
            )
            draw.text((66, y_row), row_text, font=font_body, fill=text)
            y_row += 34
        return top + 310

    y = section_title("신규 편입 TOP 10", "0%에서 플러스 비중으로 새로 편입된 종목", 460)
    y = draw_change_rows("신규 편입 종목", new_entries, y, green)
    y = section_title("평균 이상 비중 변화", "매수/매도 각각의 평균 이상 변화만 추려낸 전략 후보", y + 5)
    y = draw_change_rows("매수/증가 평균 이상", above_buys, y, red, f"평균 {format_delta(buy_average)}")
    y = draw_change_rows("매도/감소 평균 이상", above_sells, y, cyan, f"평균 -{sell_average:.2f}pp")
    y = section_title("금액 변화 TOP", "ETF별 평가금액 변화를 종목별로 합산", y + 5)
    y = draw_amount_rows("합산 매수 금액 TOP", amount_buys, y, red)
    y = draw_amount_rows("합산 매도 금액 TOP", amount_sells, y, cyan)

    if skipped:
        draw.text((42, height - 86), f"PDF 미업데이트/데이터 없음: {len(skipped)}개", font=font_small, fill=muted)
    draw.text((42, height - 48), "Generated by ETF KRX Monitor | Image report for client download", font=font_small, fill=muted)

    image.save(latest_path, "PNG", optimize=True)
    image.save(dated_path, "PNG", optimize=True)
    return latest_path


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
    image_url = html_cell(str(config.get("public_image_url", "latest_changes.png")))
    flow_html = render_amount_flow_html(changes_by_etf)

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
                label = "\uc2e0\uaddc" if is_new_entry(item) else "\uc81c\uc678" if item.change_type == "REMOVED" else ("\uc99d\uac00" if item.weight_delta > 0 else "\uac10\uc18c")
                row_side = html_signal_class(item, "new" if is_new_entry(item) else side)
                rows.append(
                    f"<tr class=\"{html_signal_row_class(item)}\">"
                    f"<td class=\"type {row_side}\">{label}</td>"
                    f"<td>{html_cell(item.holding_name)}</td>"
                    f"<td>{html_cell(item.holding_code)}</td>"
                    f"<td class=\"num\">{format_weight(item.previous_weight)}</td>"
                    f"<td class=\"num\">{format_weight(item.current_weight)}</td>"
                    f"<td class=\"num delta {row_side}\">{format_delta(item.weight_delta)}</td>"
                    f"<td class=\"num {row_side}\">{format_krw(item.amount_delta)}</td>"
                    "</tr>"
                )
            return "\n".join(rows) if rows else "<tr><td colspan=\"7\" class=\"empty\">\ud574\ub2f9 \ubcc0\ud654 \uc5c6\uc74c</td></tr>"

        sections.append(
            f"""
            <section class="etf-card" data-search="{search_text}">
              <div class="etf-head">
                <div>
                  <h2>{html_cell(etf.name)} <span>{html_cell(etf.ticker)}</span></h2>
                  <p>\ub9e4\uc218/\ube44\uc911\uc99d\uac00 {len(buys)}\uac74 | \ub9e4\ub3c4/\ube44\uc911\uac10\uc18c {len(sells)}\uac74 | \uc804\uccb4 {len(changes)}\uac74</p>
                </div>
              </div>
              <div class="tables">
                <div>
                  <h3>\ub9e4\uc218/\ube44\uc911\uc99d\uac00 \uc804\uccb4</h3>
                  <table>
                    <thead><tr><th>\uad6c\ubd84</th><th>\uc885\ubaa9\uba85</th><th>\ucf54\ub4dc</th><th>\uc774\uc804</th><th>\ud604\uc7ac</th><th>\ubcc0\ud654</th><th>\uae08\uc561\ubcc0\ud654</th></tr></thead>
                    <tbody>{render_rows(buys, 'buy')}</tbody>
                  </table>
                </div>
                <div>
                  <h3>\ub9e4\ub3c4/\ube44\uc911\uac10\uc18c \uc804\uccb4</h3>
                  <table>
                    <thead><tr><th>\uad6c\ubd84</th><th>\uc885\ubaa9\uba85</th><th>\ucf54\ub4dc</th><th>\uc774\uc804</th><th>\ud604\uc7ac</th><th>\ubcc0\ud654</th><th>\uae08\uc561\ubcc0\ud654</th></tr></thead>
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
        skipped_html = f"<details class=\"skipped\"><summary>\uc218\uc9d1 \uc2e4\ud328/\ub370\uc774\ud130 \uc5c6\uc74c {len(skipped)}\uac1c</summary><ul>{skipped_items}</ul></details>"

    no_change_html = "" if changed_etfs else "<section class=\"etf-card\"><h2>\uac10\uc9c0\ub41c \ube44\uc911 \ubcc0\ud654\uac00 \uc5c6\uc2b5\ub2c8\ub2e4.</h2></section>"
    generated_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    body = "\n".join(sections)
    document = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>\uc720\uc9c4\uc99d\uad8c \uc548\uc0c1\ud604 \uc13c\ud130\uc7a5\uc758 ETF \ubcc0\ub3d9\uc728 \uccb4\ud06c \ub300\uc26c\ubcf4\ub4dc</title>
  <style>
    :root {{ color-scheme: dark; --bg: #050608; --panel: #101318; --line: #2a3038; --line-strong: #3b434f; --text: #f2f5f8; --muted: #9aa6b2; --amber: #f5b301; --amber-soft: #2a230f; --gold: #ffd24a; --green: #19c37d; --red: #ff4d5e; --cyan: #3dd6e8; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: Arial, 'Malgun Gothic', sans-serif; background: var(--bg); color: var(--text); letter-spacing: 0; }}
    header {{ border-bottom: 1px solid var(--line-strong); background: linear-gradient(180deg, #111821 0%, #07090c 100%); padding: 18px 22px 16px; }}
    .topline {{ display: flex; align-items: center; gap: 10px; color: var(--amber); font-size: 12px; font-weight: 700; text-transform: uppercase; }}
    .ticker-dot {{ width: 9px; height: 9px; background: var(--amber); display: inline-block; }}
    header h1 {{ margin: 10px 0 8px; font-size: clamp(22px, 3vw, 34px); line-height: 1.2; font-weight: 800; }}
    header p {{ margin: 0; color: var(--muted); font-size: 14px; }}
    main {{ padding: 18px 22px 44px; }}
    .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 16px; }}
    .summary div {{ background: var(--panel); border: 1px solid var(--line); border-left: 4px solid var(--amber); border-radius: 4px; padding: 12px 13px; color: var(--muted); font-size: 12px; text-transform: uppercase; }}
    .summary b {{ display: block; color: var(--text); font-size: 26px; line-height: 1; margin-top: 8px; font-family: Consolas, 'Courier New', monospace; }}
    .tools {{ display: flex; gap: 10px; align-items: center; margin: 14px 0 10px; }}
    input {{ width: min(620px, 100%); padding: 11px 12px; border: 1px solid var(--line-strong); border-radius: 4px; background: #0b0e12; color: var(--text); font-size: 14px; outline: none; }}
    input:focus {{ border-color: var(--amber); box-shadow: 0 0 0 2px rgba(245, 179, 1, .18); }}
    .download-link {{ display: inline-block; margin-left: 10px; color: #050608; background: var(--amber); padding: 7px 10px; border-radius: 4px; font-weight: 800; text-decoration: none; }}
    .etf-card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 4px; padding: 14px; margin: 12px 0; box-shadow: 0 12px 30px rgba(0, 0, 0, .18); }}
    .etf-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: start; border-bottom: 1px solid var(--line); padding-bottom: 10px; margin-bottom: 10px; }}
    h2 {{ margin: 0; font-size: 18px; line-height: 1.35; }}
    h2 span {{ color: var(--amber); font-weight: 700; font-family: Consolas, 'Courier New', monospace; }}
    h3 {{ margin: 14px 0 8px; font-size: 13px; color: var(--cyan); font-weight: 800; text-transform: uppercase; }}
    p {{ margin: 6px 0 0; color: var(--muted); }}
    .tables {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 14px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; background: #0b0e12; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 8px 7px; text-align: left; }}
    th {{ background: #191f27; color: var(--amber); position: sticky; top: 0; font-size: 11px; text-transform: uppercase; }}
    tr:hover td {{ background: #111821; }}
    .num {{ text-align: right; white-space: nowrap; font-family: Consolas, 'Courier New', monospace; }}
    .type {{ font-weight: 800; white-space: nowrap; font-family: Consolas, 'Courier New', monospace; }}
    .buy {{ color: var(--red); }}
    .sell {{ color: var(--cyan); }}
    .new {{ color: var(--green); }}
    .gold {{ color: var(--gold); font-weight: 900; }}
    tr.gold-row td {{ color: var(--gold); font-weight: 900; }}
    .empty {{ color: var(--muted); text-align: center; }}
    .new-entry-board {{ margin-top: 16px; overflow-x: auto; }}
    .removed-entry-board {{ margin-top: 16px; overflow-x: auto; }}
    .average-change-board {{ margin-top: 16px; overflow-x: auto; }}
    .skipped {{ background: var(--amber-soft); border: 1px solid #7c5a00; border-radius: 4px; padding: 12px 14px; margin-bottom: 14px; color: var(--text); }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    @media (max-width: 720px) {{ main {{ padding: 14px; }} header {{ padding: 16px 14px; }} .tables {{ grid-template-columns: 1fr; overflow-x: auto; }} table {{ min-width: 620px; }} .download-link {{ margin: 8px 0 0; }} }}
  </style>
</head>
<body>
<header>
  <div class="topline"><span class="ticker-dot"></span> EUGENE SECURITIES | ACTIVE ETF MONITOR</div>
  <h1>\uc720\uc9c4\uc99d\uad8c \uc548\uc0c1\ud604 \uc13c\ud130\uc7a5\uc758 ETF \ubcc0\ub3d9\uc728 \uccb4\ud06c \ub300\uc26c\ubcf4\ub4dc</h1>
  <p>{pretty_date} \uae30\uc900 | \uc0dd\uc131 {generated_at} | KRX PDF \ubcf4\uc720\uc885\ubaa9 \ubcc0\ud654</p>
</header>
<main>
  <div class="summary">
    <div>\ub300\uc0c1 ETF<b>{len(etfs)}</b></div>
    <div>\ubcc0\ud654 ETF<b>{len(changed_etfs)}</b></div>
    <div>\uc804\uccb4 \ubcc0\ud654<b>{total_changes}</b></div>
    <div>\ub9e4\uc218/\uc99d\uac00<b>{total_buy}</b></div>
    <div>\ub9e4\ub3c4/\uac10\uc18c<b>{total_sell}</b></div>
  </div>
  <div class="tools"><input id="search" placeholder="ETF\uba85, ETF\ucf54\ub4dc, \uc885\ubaa9\uba85, \uc885\ubaa9\ucf54\ub4dc \uac80\uc0c9"></div>
  <p class="muted">\ud154\ub808\uadf8\ub7a8\uc740 \uc694\uc57d\ub9cc \ubcf4\ub0b4\uace0, \uc774 HTML\uc5d0\ub294 \uac10\uc9c0\ub41c \ubaa8\ub4e0 \ubcc0\ud654\uac00 \ud45c\uc2dc\ub429\ub2c8\ub2e4. \uc2e0\uaddc \ud3b8\uc785 \uc911 \uae08\uc561 \ubcc0\ud654 1,000\ub9cc\uc6d0 \uc774\uc0c1\uc740 \uae08\uc0c9\uc73c\ub85c \uac15\uc870\ud569\ub2c8\ub2e4. <a class="download-link" href="{image_url}" download>\uc774\ubbf8\uc9c0 \ubcf4\uace0\uc11c \ub2e4\uc6b4\ub85c\ub4dc</a></p>
  {flow_html}
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
                        skipped.append(f"{etf.name}({etf.ticker}): \ub370\uc774\ud130 \uc5c6\uc74c \ub610\ub294 PDF \ubbf8\uc5c5\ub370\uc774\ud2b8")
                        continue
                    prev_date = previous_trade_date_in_db(conn, etf.ticker, actual_date)
                    previous_holdings = load_holdings_from_db(conn, etf.ticker, prev_date) if prev_date else {}
                    current_holdings = holding_compare_rows(holdings)
                    reason = unready_pdf_reason(etf, previous_holdings, current_holdings, config) if prev_date else None
                    if reason:
                        existing_current = load_holdings_from_db(conn, etf.ticker, actual_date)
                        if unready_pdf_reason(etf, previous_holdings, existing_current, config):
                            delete_holdings_for_date(conn, etf, actual_date)
                        skipped.append(f"{etf.name}({etf.ticker}): {reason}")
                        continue
                    replace_holdings(conn, etf, actual_date, holdings)
                    if not prev_date:
                        continue
                    changes = compare_holdings(
                        etf,
                        actual_date,
                        previous_holdings,
                        current_holdings,
                        min_delta=float(config.get("min_weight_delta_pp", 0.05)),
                    )
                    all_changes.extend(changes)
                except Exception as exc:
                    skipped.append(f"{etf.name}({etf.ticker}): {exc}")
            save_changes(conn, run_id, all_changes)
            save_new_entries(conn, run_id, all_changes)
            save_daily_new_entries(conn, run_id, trade_date, all_changes)
            save_removed_entries(conn, run_id, all_changes)
            save_daily_removed_entries(conn, run_id, trade_date, all_changes)
            save_above_average_changes(conn, run_id, all_changes)
            changes_by_etf: dict[str, list[Change]] = {}
            for change in all_changes:
                changes_by_etf.setdefault(change.etf_ticker, []).append(change)
            report = build_report(trade_date, etfs, changes_by_etf, skipped, config)
            html_path = build_html_report(trade_date, etfs, changes_by_etf, skipped, config, run_id)
            image_path = build_image_report(trade_date, etfs, changes_by_etf, skipped, config, run_id)
            public_report_url = str(config.get("public_report_url", "https://se2in.github.io/ETF_KRX/")).strip()
            public_image_url = str(config.get("public_image_url", "https://se2in.github.io/ETF_KRX/latest_changes.png")).strip()
            report = f"{report}\n\n\uc804\uccb4 HTML \ub9ac\ud3ec\ud2b8: {public_report_url}\n\uc774\ubbf8\uc9c0 \ubcf4\uace0\uc11c: {public_image_url}"
            finish_run(conn, run_id, "SUCCESS", f"{len(all_changes)} changes; html={html_path}; image={image_path}")
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






