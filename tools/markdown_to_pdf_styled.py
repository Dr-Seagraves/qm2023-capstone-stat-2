from pathlib import Path
import re
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.utils import ImageReader
from reportlab.lib.units import inch
from reportlab.platypus import Image as RLImage, ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def md_inline_format(text: str) -> str:
    # Keep the PDF visually consistent and avoid ReportLab parser issues.
    text = re.sub(r"\*\*\*(.+?)\*\*\*", r"\1", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"`(.+?)`", r"\1", text)
    return escape(text, {"'": "&apos;", '"': "&quot;"})


def is_table_separator(line: str) -> bool:
    stripped = line.strip()
    if '|' not in stripped:
        return False
    return bool(re.fullmatch(r"\|?(\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?", stripped))


def parse_table_row(line: str):
    cells = [cell.strip() for cell in line.strip().strip('|').split('|')]
    return [md_inline_format(cell) for cell in cells]


def parse_image_line(line: str):
    match = re.fullmatch(r'!\[(.*?)\]\((.*?)\)', line.strip())
    if not match:
        return None
    return match.group(1).strip(), match.group(2).strip()


def parse_markdown(md_text: str):
    lines = md_text.splitlines()
    blocks = []
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line:
            i += 1
            continue
        if line.startswith('# '):
            blocks.append(('h1', line.lstrip('# ').strip()))
            i += 1
            continue
        if line.startswith('## '):
            blocks.append(('h2', line.lstrip('# ').strip()))
            i += 1
            continue
        if line.startswith('### '):
            blocks.append(('h3', line.lstrip('# ').strip()))
            i += 1
            continue
        if line.startswith('- '):
            items = []
            while i < len(lines) and lines[i].lstrip().startswith('- '):
                items.append(lines[i].lstrip()[2:].strip())
                i += 1
            blocks.append(('ul', items))
            continue
        image_info = parse_image_line(line)
        if image_info:
            blocks.append(('image', {'caption': image_info[0], 'path': image_info[1]}))
            i += 1
            continue
        if '|' in line and i + 1 < len(lines) and is_table_separator(lines[i + 1]):
            rows = [parse_table_row(line)]
            i += 2
            while i < len(lines):
                candidate = lines[i].rstrip()
                if not candidate.strip() or '|' not in candidate:
                    break
                rows.append(parse_table_row(candidate))
                i += 1
            blocks.append(('table', rows))
            continue
        # Paragraph: collect until blank line
        para_lines = [line]
        i += 1
        while i < len(lines) and lines[i].strip():
            para_lines.append(lines[i].strip())
            i += 1
        blocks.append(('p', ' '.join(para_lines)))
    return blocks


def render_pdf(md_path: Path, pdf_path: Path, repo_root: Path):
    text = md_path.read_text(encoding='utf-8')
    blocks = parse_markdown(text)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'Title',
        parent=styles['Heading1'],
        fontName='Times-Bold',
        fontSize=15,
        leading=18,
        alignment=TA_CENTER,
        spaceAfter=10,
    )
    heading_style = ParagraphStyle(
        'Heading',
        parent=styles['Heading2'],
        fontName='Times-Bold',
        fontSize=14,
        leading=17,
        alignment=TA_LEFT,
        spaceBefore=8,
        spaceAfter=5,
    )
    subheading_style = ParagraphStyle(
        'Subheading',
        parent=styles['Heading3'],
        fontName='Times-Bold',
        fontSize=12,
        leading=15,
        alignment=TA_LEFT,
        spaceBefore=6,
        spaceAfter=4,
    )
    body_style = ParagraphStyle(
        'Body',
        parent=styles['BodyText'],
        fontName='Times-Roman',
        fontSize=9.5,
        leading=12.5,
        alignment=TA_JUSTIFY,
        spaceAfter=3,
    )
    bullet_style = ParagraphStyle(
        'Bullet',
        parent=styles['BodyText'],
        fontName='Times-Roman',
        fontSize=9.5,
        leading=12.5,
        leftIndent=16,
        firstLineIndent=0,
        alignment=TA_JUSTIFY,
        spaceAfter=2,
    )
    caption_style = ParagraphStyle(
        'Caption',
        parent=styles['BodyText'],
        fontName='Times-Roman',
        fontSize=8,
        leading=9.5,
        alignment=TA_CENTER,
        spaceAfter=2,
    )

    doc = SimpleDocTemplate(str(pdf_path), pagesize=letter,
                            rightMargin=1*inch, leftMargin=1*inch,
                            topMargin=1*inch, bottomMargin=1*inch)

    flow = []

    # If first block is a title-like h1 use centered title
    if blocks and blocks[0][0] == 'h1':
        flow.append(Paragraph(md_inline_format(blocks[0][1]), title_style))
        flow.append(Spacer(1, 0.08*inch))
        blocks = blocks[1:]

    for kind, content in blocks:
        if kind == 'h1':
            flow.append(Paragraph(md_inline_format(content), heading_style))
            flow.append(Spacer(1, 0.04*inch))
        elif kind == 'h2' or kind == 'h3':
            flow.append(Paragraph(md_inline_format(content), subheading_style))
            flow.append(Spacer(1, 0.03*inch))
        elif kind == 'p':
            flow.append(Paragraph(md_inline_format(content), body_style))
            flow.append(Spacer(1, 0.025*inch))
        elif kind == 'ul':
            items = [ListItem(Paragraph(md_inline_format(it), bullet_style), leftIndent=6) for it in content]
            lf = ListFlowable(items, bulletType='bullet', start='•', leftIndent=16)
            flow.append(lf)
            flow.append(Spacer(1, 0.025*inch))
        elif kind == 'table':
            table_data = [[Paragraph(cell, body_style) for cell in row] for row in content]
            column_count = max(len(row) for row in table_data)
            normalized_rows = []
            for row in table_data:
                normalized_rows.append(row + [''] * (column_count - len(row)))
            table = Table(normalized_rows, colWidths=[doc.width / column_count] * column_count, repeatRows=1)
            table.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), 'Times-Roman'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('LEADING', (0, 0), (-1, -1), 11.5),
                ('FONTNAME', (0, 0), (-1, 0), 'Times-Bold'),
                ('BACKGROUND', (0, 0), (-1, 0), colors.whitesmoke),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 3),
                ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            flow.append(table)
            flow.append(Spacer(1, 0.04*inch))
        elif kind == 'image':
            image_path = repo_root / content['path']
            if image_path.exists():
                image_width, image_height = ImageReader(str(image_path)).getSize()
                aspect = image_height / image_width if image_width else 1
                max_width = doc.width
                max_height = 1.55 * inch
                draw_width = min(max_width, max_height / aspect)
                draw_height = draw_width * aspect
                flow.append(RLImage(str(image_path), width=draw_width, height=draw_height))
                flow.append(Spacer(1, 0.015 * inch))
                if content.get('caption'):
                    flow.append(Paragraph(md_inline_format(content['caption']), caption_style))
                flow.append(Spacer(1, 0.02 * inch))

    doc.build(flow)


def main():
    repo_root = Path(__file__).resolve().parents[1]
    out_dir = repo_root / 'results'
    out_dir.mkdir(parents=True, exist_ok=True)

    files = [
        (repo_root / 'Final_Investment_Memo.md', out_dir / 'Final_Investment_Memo.pdf'),
        (repo_root / 'Individual Addendums' / 'Individual_Addendum_Dani_Gamboa.md', out_dir / 'Individual_Addendum_Dani_Gamboa.pdf'),
    ]

    for md, pdf in files:
        if not md.exists():
            print(f"Source not found: {md}")
            continue
        if pdf.exists():
            pdf.unlink()
        print(f"Rendering {md} -> {pdf}")
        render_pdf(md, pdf, repo_root)
        print(f"Wrote {pdf}")


if __name__ == '__main__':
    main()
