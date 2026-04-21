from __future__ import annotations

import html
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
EMU_PER_PX = 9525


def xml(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def emu(value: float) -> int:
    return int(round(value * EMU_PER_PX))


def format_weight(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}%"


def format_delta(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}pp"


class Slide:
    def __init__(self, background: str = "050608") -> None:
        self.background = background.strip("#")
        self.shapes: list[str] = []
        self.shape_id = 2

    def _next_id(self) -> int:
        value = self.shape_id
        self.shape_id += 1
        return value

    def rect(self, left: float, top: float, width: float, height: float, fill: str, line: str = "2A3038") -> None:
        sid = self._next_id()
        self.shapes.append(
            f'<p:sp><p:nvSpPr><p:cNvPr id="{sid}" name="Panel {sid}"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>'
            f'<p:spPr><a:xfrm><a:off x="{emu(left)}" y="{emu(top)}"/><a:ext cx="{emu(width)}" cy="{emu(height)}"/></a:xfrm>'
            f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom><a:solidFill><a:srgbClr val="{fill.strip("#")}"/></a:solidFill>'
            f'<a:ln w="9525"><a:solidFill><a:srgbClr val="{line.strip("#")}"/></a:solidFill></a:ln></p:spPr>'
            f'<p:txBody><a:bodyPr/><a:lstStyle/><a:p/></p:txBody></p:sp>'
        )

    def text(
        self,
        left: float,
        top: float,
        width: float,
        height: float,
        value: str,
        font_size: int = 24,
        color: str = "F2F5F8",
        bold: bool = False,
        fill: str | None = None,
        line: str | None = None,
        align: str = "l",
        valign: str = "top",
    ) -> None:
        sid = self._next_id()
        fill_xml = "<a:noFill/>" if fill is None else f'<a:solidFill><a:srgbClr val="{fill.strip("#")}"/></a:solidFill>'
        line_xml = "<a:ln><a:noFill/></a:ln>" if line is None else f'<a:ln w="9525"><a:solidFill><a:srgbClr val="{line.strip("#")}"/></a:solidFill></a:ln>'
        anchor = "mid" if valign == "middle" else "t"
        bold_attr = ' b="1"' if bold else ""
        paragraphs = []
        for raw_line in str(value).splitlines() or [""]:
            paragraphs.append(
                f'<a:p><a:pPr algn="{align}"/><a:r><a:rPr lang="ko-KR" sz="{font_size * 100}"{bold_attr}>'
                f'<a:solidFill><a:srgbClr val="{color.strip("#")}"/></a:solidFill><a:latin typeface="Malgun Gothic"/><a:ea typeface="Malgun Gothic"/></a:rPr>'
                f'<a:t>{xml(raw_line)}</a:t></a:r><a:endParaRPr lang="ko-KR" sz="{font_size * 100}"/></a:p>'
            )
        body = "".join(paragraphs)
        self.shapes.append(
            f'<p:sp><p:nvSpPr><p:cNvPr id="{sid}" name="Text {sid}"/><p:cNvSpPr txBox="1"/><p:nvPr/></p:nvSpPr>'
            f'<p:spPr><a:xfrm><a:off x="{emu(left)}" y="{emu(top)}"/><a:ext cx="{emu(width)}" cy="{emu(height)}"/></a:xfrm>'
            f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom>{fill_xml}{line_xml}</p:spPr>'
            f'<p:txBody><a:bodyPr wrap="square" anchor="{anchor}"><a:spAutoFit/></a:bodyPr><a:lstStyle/>{body}</p:txBody></p:sp>'
        )

    def to_xml(self) -> str:
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">'
            f'<p:cSld><p:bg><p:bgPr><a:solidFill><a:srgbClr val="{self.background}"/></a:solidFill><a:effectLst/></p:bgPr></p:bg>'
            '<p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr>'
            '<p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>'
            f'{"".join(self.shapes)}</p:spTree></p:cSld><p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr></p:sld>'
        )


def add_header(slide: Slide, kicker: str, title: str, subtitle: str) -> None:
    slide.text(34, 20, 540, 24, kicker, 13, "F5B301", True)
    slide.text(34, 50, 980, 64, title, 30, "F2F5F8", True)
    slide.text(36, 114, 800, 28, subtitle, 15, "9AA6B2")
    slide.rect(34, 150, 1210, 2, "F5B301", "F5B301")


def metric(slide: Slide, left: float, top: float, label: str, value: str, accent: str = "F5B301") -> None:
    slide.rect(left, top, 220, 96, "101318", "2A3038")
    slide.rect(left, top, 6, 96, accent, accent)
    slide.text(left + 18, top + 14, 185, 20, label, 12, "9AA6B2", True)
    slide.text(left + 18, top + 38, 180, 42, value, 30, "F2F5F8", True)


def table(slide: Slide, left: float, top: float, widths: list[int], rows: list[list[str]], row_h: int = 38) -> None:
    x = left
    for col, width in enumerate(widths):
        slide.rect(x, top, width, row_h, "191F27", "2A3038")
        slide.text(x + 7, top + 9, width - 14, 18, rows[0][col], 11, "F5B301", True)
        x += width
    for r, row in enumerate(rows[1:], start=1):
        x = left
        fill = "0B0E12" if r % 2 else "101318"
        for col, width in enumerate(widths):
            slide.rect(x, top + r * row_h, width, row_h, fill, "2A3038")
            color = "19C37D" if (col == len(widths) - 1 and str(row[col]).startswith("+")) else "FF4D5E" if col == len(widths) - 1 else "F2F5F8"
            align = "r" if col >= len(widths) - 3 else "l"
            slide.text(x + 7, top + r * row_h + 9, width - 14, 18, row[col], 11, color, col == len(widths) - 1, align=align)
            x += width


def build_pptx_report(trade_date: str, etfs: list[Any], changes_by_etf: dict[str, list[Any]], skipped: list[str], config: dict[str, Any], run_id: int) -> Path:
    pretty_date = datetime.strptime(trade_date, "%Y%m%d").strftime("%Y-%m-%d")
    changed_etfs = [etf for etf in etfs if changes_by_etf.get(etf.ticker)]
    all_changes = [change for changes in changes_by_etf.values() for change in changes]
    buys = sorted([item for item in all_changes if item.weight_delta > 0], key=lambda item: item.weight_delta, reverse=True)
    sells = sorted([item for item in all_changes if item.weight_delta < 0], key=lambda item: item.weight_delta)
    total_changes = len(all_changes)
    total_buy = len(buys)
    total_sell = len(sells)

    latest_path = Path(str(config.get("pptx_report_path", "reports/latest_changes.pptx")))
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    dated_path = latest_path.parent / f"changes_{trade_date}_run_{run_id}.pptx"

    etf_summaries = []
    for etf in changed_etfs:
        changes = changes_by_etf[etf.ticker]
        etf_summaries.append((sum(abs(item.weight_delta) for item in changes), etf, changes))
    etf_summaries.sort(key=lambda item: item[0], reverse=True)

    slides: list[Slide] = []

    slide = Slide()
    headline = "\uc624\ub298 ETF \uc790\uae08\uc758 \ud754\uc801, \uc5ec\uae30\uc11c \uac08\ub838\uc2b5\ub2c8\ub2e4" if total_changes else "\uc624\ub298\uc740 \ud070 \uc190\uc758 \ud754\uc801\uc774 \uc870\uc6a9\ud588\uc2b5\ub2c8\ub2e4"
    add_header(slide, "YUJIN SECURITIES | ACTIVE ETF RADAR", headline, f"{pretty_date} KRX PDF \ubcf4\uc720\uc885\ubaa9 \ubcc0\ud654 \uc694\uc57d")
    metric(slide, 48, 190, "\ub300\uc0c1 ETF", str(len(etfs)))
    metric(slide, 292, 190, "\ubcc0\ud654 ETF", str(len(changed_etfs)), "3DD6E8")
    metric(slide, 536, 190, "\uc804\uccb4 \ubcc0\ud654", str(total_changes))
    metric(slide, 780, 190, "\ub9e4\uc218/\uc99d\uac00", str(total_buy), "19C37D")
    metric(slide, 1024, 190, "\ub9e4\ub3c4/\uac10\uc18c", str(total_sell), "FF4D5E")
    slide.text(60, 342, 1110, 72, "\uc190\ub2d8\uc774 \ubc14\ub85c \ubcfc \uc9c0\uc810: \uc5b4\ub290 ETF\uc5d0\uc11c \ubb34\uc5c7\uc744 \ub354 \ub2f4\uace0, \ubb34\uc5c7\uc744 \uc904\uc600\ub294\uc9c0\ub97c \ud55c \ubc88\uc5d0 \ubcf4\uc5ec\uc8fc\ub294 \uc790\ub8cc\uc785\ub2c8\ub2e4.", 24, "F2F5F8", True, "101318", "2A3038")
    slide.text(60, 452, 1110, 110, "\ud22c\uc790 \uad8c\uc720\uac00 \uc544\ub2c8\ub77c, KRX PDF \ubcf4\uc720\uc885\ubaa9 \ubcc0\ud654\ub97c \uc694\uc57d\ud55c \uccb4\ud06c\uc6a9 \ub9ac\ud3ec\ud2b8\uc785\ub2c8\ub2e4.\n\uc0c1\uc138 \uc804\uccb4 \ubcc0\ud654\ub294 HTML \ub300\uc26c\ubcf4\ub4dc\uc5d0\uc11c \ud655\uc778\ud558\uc138\uc694.", 17, "9AA6B2")
    slides.append(slide)

    slide = Slide()
    add_header(slide, "MONEY FLOW HEAT", "\uc624\ub298 \uac00\uc7a5 \uc138\uac8c \uc6c0\uc9c1\uc778 ETF", f"\ubcc0\ud654 \uac15\ub3c4 \uc0c1\uc704 {min(5, len(etf_summaries))}\uac1c")
    if etf_summaries:
        for idx, (score, etf, changes) in enumerate(etf_summaries[:5], start=1):
            top = 176 + (idx - 1) * 90
            buy_count = len([item for item in changes if item.weight_delta > 0])
            sell_count = len([item for item in changes if item.weight_delta < 0])
            slide.rect(54, top, 1168, 72, "101318", "2A3038")
            slide.text(72, top + 14, 54, 30, f"#{idx}", 23, "F5B301", True)
            slide.text(138, top + 12, 560, 24, f"{etf.name} ({etf.ticker})", 18, "F2F5F8", True)
            slide.text(138, top + 40, 620, 20, f"\ubcc0\ud654 {len(changes)}\uac74 | \ub9e4\uc218/\uc99d\uac00 {buy_count}\uac74 | \ub9e4\ub3c4/\uac10\uc18c {sell_count}\uac74", 13, "9AA6B2")
            slide.text(952, top + 18, 230, 26, f"\ucda9\uaca9\ub3c4 {score:.2f}pp", 18, "3DD6E8", True, align="r")
    else:
        slide.text(80, 230, 1080, 90, "\uac10\uc9c0\ub41c \ube44\uc911 \ubcc0\ud654\uac00 \uc5c6\uc2b5\ub2c8\ub2e4.\n\uc774\ub7f0 \ub0a0\uc740 \uc804\ub7b5\uc758 \ubcc0\ud654\ubcf4\ub2e4 \uc9c0\uc18d\uc131\uc744 \ud655\uc778\ud558\ub294 \ub0a0\uc785\ub2c8\ub2e4.", 26, "F2F5F8", True)
    slides.append(slide)

    slide = Slide()
    add_header(slide, "BUY / INCREASE WATCH", "\ube44\uc911\uc774 \ub298\uc5b4\ub09c \uc885\ubaa9 TOP 10", "\uc2e0\uaddc \ud3b8\uc785\uacfc \ube44\uc911 \uc99d\uac00\ub97c \ud568\uaed8 \ud45c\uc2dc")
    rows = [["ETF", "\uc885\ubaa9", "\ucf54\ub4dc", "\uc774\uc804", "\ud604\uc7ac", "\ubcc0\ud654"]]
    for item in buys[:10]:
        label = "\uc2e0\uaddc" if item.change_type == "ADDED" else "\uc99d\uac00"
        rows.append([item.etf_name[:18], f"{label} {item.holding_name[:18]}", item.holding_code, format_weight(item.previous_weight), format_weight(item.current_weight), format_delta(item.weight_delta)])
    if len(rows) == 1:
        rows.append(["-", "\ud574\ub2f9 \ubcc0\ud654 \uc5c6\uc74c", "-", "-", "-", "-"])
    table(slide, 42, 175, [250, 325, 110, 120, 120, 135], rows)
    slides.append(slide)

    slide = Slide()
    add_header(slide, "SELL / DECREASE WATCH", "\ube44\uc911\uc774 \uc904\uc5b4\ub4e0 \uc885\ubaa9 TOP 10", "\uc81c\uc678 \uc885\ubaa9\uacfc \ube44\uc911 \uac10\uc18c\ub97c \ud568\uaed8 \ud45c\uc2dc")
    rows = [["ETF", "\uc885\ubaa9", "\ucf54\ub4dc", "\uc774\uc804", "\ud604\uc7ac", "\ubcc0\ud654"]]
    for item in sells[:10]:
        label = "\uc81c\uc678" if item.change_type == "REMOVED" else "\uac10\uc18c"
        rows.append([item.etf_name[:18], f"{label} {item.holding_name[:18]}", item.holding_code, format_weight(item.previous_weight), format_weight(item.current_weight), format_delta(item.weight_delta)])
    if len(rows) == 1:
        rows.append(["-", "\ud574\ub2f9 \ubcc0\ud654 \uc5c6\uc74c", "-", "-", "-", "-"])
    table(slide, 42, 175, [250, 325, 110, 120, 120, 135], rows)
    slides.append(slide)

    slide = Slide()
    add_header(slide, "CLIENT BRIEF", "\uc190\ub2d8\uc5d0\uac8c \uc774\ub807\uac8c \ubcf4\uc5ec\uc8fc\uc138\uc694", "\uc9e7\uace0 \uc138\uac8c \uc77d\ud788\ub294 \uc694\uc57d")
    bullets = [
        f"1. \uc624\ub298 \ubcc0\ud654 ETF\ub294 {len(changed_etfs)}\uac1c, \uc804\uccb4 \ubcc0\ud654\ub294 {total_changes}\uac74\uc785\ub2c8\ub2e4.",
        f"2. \ub9e4\uc218/\ube44\uc911\uc99d\uac00 {total_buy}\uac74, \ub9e4\ub3c4/\ube44\uc911\uac10\uc18c {total_sell}\uac74\uc744 \ud655\uc778\ud588\uc2b5\ub2c8\ub2e4.",
        "3. \uc0c1\uc704 \uc885\ubaa9\uc740 \ubcf4\uc720\uc885\ubaa9 \ubcc0\ud654\uc758 \ubc29\ud5a5\uc131\uc744 \ubcf4\ub294 \uccb4\ud06c\ud3ec\uc778\ud2b8\uc785\ub2c8\ub2e4.",
        "4. \uc804\uccb4 \uc0c1\uc138 \ub0b4\uc5ed\uc740 HTML \ub300\uc26c\ubcf4\ub4dc\uc5d0\uc11c ETF\ubcc4\ub85c \uac80\uc0c9\ud558\uba74 \ub429\ub2c8\ub2e4.",
    ]
    slide.text(74, 188, 1110, 240, "\n".join(bullets), 24, "F2F5F8", True, "101318", "2A3038")
    slide.text(74, 480, 1110, 80, "\uc790\uadf9\uc801\uc73c\ub85c \ubcf4\uc774\uc9c0\ub9cc, \ub0b4\uc6a9\uc740 \uc624\uc9c1 KRX PDF \ub370\uc774\ud130 \ubcc0\ud654 \uae30\ub85d\uc785\ub2c8\ub2e4. \ud22c\uc790\ud310\ub2e8\uc740 \ubcc4\ub3c4\uc758 \uc0c1\ub2f4\uacfc \ud568\uaed8 \ud558\uc138\uc694.", 17, "9AA6B2")
    slides.append(slide)

    title = "\uc720\uc9c4\uc99d\uad8c \uc548\uc0c1\ud604 \uc13c\ud130\uc7a5\uc758 ETF \ubcc0\ub3d9\uc728 \uccb4\ud06c \ubcf4\uace0\uc11c"
    write_pptx(slides, latest_path, title)
    write_pptx(slides, dated_path, title)
    return latest_path


def write_pptx(slides: list[Slide], output_path: Path, title: str) -> None:
    slide_overrides = "".join(f'<Override PartName="/ppt/slides/slide{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>' for i in range(1, len(slides) + 1))
    sld_ids = "".join(f'<p:sldId id="{255 + i}" r:id="rId{i + 1}"/>' for i in range(1, len(slides) + 1))
    pres_rels = ['<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster" Target="slideMasters/slideMaster1.xml"/>']
    pres_rels.extend(f'<Relationship Id="rId{i + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" Target="slides/slide{i}.xml"/>' for i in range(1, len(slides) + 1))
    now = datetime.now(KST).isoformat(timespec="seconds")

    files = {
        "[Content_Types].xml": f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/ppt/presentation.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/><Override PartName="/ppt/slideMasters/slideMaster1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slideMaster+xml"/><Override PartName="/ppt/slideLayouts/slideLayout1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slideLayout+xml"/><Override PartName="/ppt/theme/theme1.xml" ContentType="application/vnd.openxmlformats-officedocument.theme+xml"/>{slide_overrides}</Types>',
        "_rels/.rels": '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="ppt/presentation.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/></Relationships>',
        "docProps/app.xml": f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"><Application>Microsoft PowerPoint</Application><PresentationFormat>On-screen Show (16:9)</PresentationFormat><Slides>{len(slides)}</Slides></Properties>',
        "docProps/core.xml": f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:title>{xml(title)}</dc:title><dc:creator>ETF KRX Monitor</dc:creator><cp:lastModifiedBy>ETF KRX Monitor</cp:lastModifiedBy><dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified></cp:coreProperties>',
        "ppt/presentation.xml": f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><p:presentation xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"><p:sldMasterIdLst><p:sldMasterId id="2147483648" r:id="rId1"/></p:sldMasterIdLst><p:sldIdLst>{sld_ids}</p:sldIdLst><p:sldSz cx="12192000" cy="6858000" type="screen16x9"/><p:notesSz cx="6858000" cy="9144000"/></p:presentation>',
        "ppt/_rels/presentation.xml.rels": f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{"".join(pres_rels)}</Relationships>',
        "ppt/slideMasters/slideMaster1.xml": '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><p:sldMaster xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"><p:cSld><p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr><p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr></p:spTree></p:cSld><p:sldLayoutIdLst><p:sldLayoutId id="2147483649" r:id="rId1"/></p:sldLayoutIdLst><p:txStyles><p:titleStyle/><p:bodyStyle/><p:otherStyle/></p:txStyles></p:sldMaster>',
        "ppt/slideMasters/_rels/slideMaster1.xml.rels": '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" Target="../slideLayouts/slideLayout1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme" Target="../theme/theme1.xml"/></Relationships>',
        "ppt/slideLayouts/slideLayout1.xml": '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><p:sldLayout xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" type="blank" preserve="1"><p:cSld name="Blank"><p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr><p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr></p:spTree></p:cSld></p:sldLayout>',
        "ppt/slideLayouts/_rels/slideLayout1.xml.rels": '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster" Target="../slideMasters/slideMaster1.xml"/></Relationships>',
        "ppt/theme/theme1.xml": '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><a:theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" name="ETF Terminal"><a:themeElements><a:clrScheme name="ETF Terminal"><a:dk1><a:srgbClr val="050608"/></a:dk1><a:lt1><a:srgbClr val="F2F5F8"/></a:lt1><a:dk2><a:srgbClr val="101318"/></a:dk2><a:lt2><a:srgbClr val="9AA6B2"/></a:lt2><a:accent1><a:srgbClr val="F5B301"/></a:accent1><a:accent2><a:srgbClr val="19C37D"/></a:accent2><a:accent3><a:srgbClr val="FF4D5E"/></a:accent3><a:accent4><a:srgbClr val="3DD6E8"/></a:accent4><a:accent5><a:srgbClr val="2A3038"/></a:accent5><a:accent6><a:srgbClr val="FFFFFF"/></a:accent6><a:hlink><a:srgbClr val="3DD6E8"/></a:hlink><a:folHlink><a:srgbClr val="F5B301"/></a:folHlink></a:clrScheme><a:fontScheme name="ETF Font"><a:majorFont><a:latin typeface="Malgun Gothic"/><a:ea typeface="Malgun Gothic"/></a:majorFont><a:minorFont><a:latin typeface="Malgun Gothic"/><a:ea typeface="Malgun Gothic"/></a:minorFont></a:fontScheme><a:fmtScheme name="ETF"><a:fillStyleLst><a:solidFill><a:schemeClr val="accent1"/></a:solidFill></a:fillStyleLst><a:lnStyleLst><a:ln w="9525"><a:solidFill><a:schemeClr val="accent5"/></a:solidFill></a:ln></a:lnStyleLst><a:effectStyleLst><a:effectStyle><a:effectLst/></a:effectStyle></a:effectStyleLst><a:bgFillStyleLst><a:solidFill><a:schemeClr val="dk1"/></a:solidFill></a:bgFillStyleLst></a:fmtScheme></a:themeElements></a:theme>',
    }

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as pptx:
        for name, content in files.items():
            pptx.writestr(name, content.encode("utf-8"))
        for idx, slide in enumerate(slides, start=1):
            pptx.writestr(f"ppt/slides/slide{idx}.xml", slide.to_xml().encode("utf-8"))
            pptx.writestr(f"ppt/slides/_rels/slide{idx}.xml.rels", b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" Target="../slideLayouts/slideLayout1.xml"/></Relationships>')
