from pathlib import Path
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
import textwrap
import sys


def render_markdown_to_pdf(md_path: Path, pdf_path: Path, page_size=letter):
    text = md_path.read_text(encoding="utf-8")

    c = canvas.Canvas(str(pdf_path), pagesize=page_size)
    width, height = page_size
    left_margin = 0.75 * inch
    right_margin = 0.75 * inch
    usable_width = width - left_margin - right_margin
    y = height - 0.75 * inch

    def new_page():
        nonlocal y
        c.showPage()
        y = height - 0.75 * inch

    def draw_line(s, font_name="Helvetica", font_size=11, leading=14):
        nonlocal y
        lines = textwrap.wrap(s, width=int(usable_width / (font_size * 0.55)))
        for ln in lines:
            if y < 0.75 * inch:
                new_page()
            c.setFont(font_name, font_size)
            c.drawString(left_margin, y, ln)
            y -= leading

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            y -= 6
            continue
        if line.startswith('# '):
            title = line.lstrip('# ').strip()
            draw_line(title, font_name="Helvetica-Bold", font_size=18, leading=22)
            y -= 6
        elif line.startswith('## '):
            h = line.lstrip('# ').strip()
            draw_line(h, font_name="Helvetica-Bold", font_size=14, leading=18)
            y -= 4
        elif line.startswith('### '):
            h = line.lstrip('# ').strip()
            draw_line(h, font_name="Helvetica-Bold", font_size=12, leading=16)
            y -= 4
        elif line.startswith('- '):
            bullet = '• ' + line[2:].strip()
            draw_line(bullet, font_name="Helvetica", font_size=11, leading=14)
        else:
            draw_line(line, font_name="Helvetica", font_size=11, leading=14)

    c.save()


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
        print(f"Rendering {md} -> {pdf}")
        try:
            render_markdown_to_pdf(md, pdf)
            print(f"Wrote {pdf}")
        except Exception as e:
            print(f"Failed to render {md}: {e}")


if __name__ == '__main__':
    main()
