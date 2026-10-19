"""Markdown-to-PDF export for Thunders reports.

Primary path uses ``fpdf2`` (https://py-pdf.github.io/fpdf2/). A
``weasyprint``-based renderer with full CSS/styling support is planned; see
the TODO below.

Usage::

    python analytics/reports/export_pdf.py [markdown_path] [pdf_path]
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

LOGGER = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

# Strips simple Markdown constructs; keeps plain text for fpdf2 core fonts.
_MD_PATTERNS = [
    (re.compile(r"^#{1,6}\s+", re.MULTILINE), ""),      # headings
    (re.compile(r"\*\*(.+?)\*\*"), r"\1"),              # bold
    (re.compile(r"\*(.+?)\*"), r"\1"),                  # italics
    (re.compile(r"`(.+?)`"), r"\1"),                    # inline code
    (re.compile(r"^\|", re.MULTILINE), "  "),           # table pipes -> indent
    (re.compile(r"^[-*_]{3,}\s*$", re.MULTILINE), "-" * 40),  # hr marker
    (re.compile(r"^>\s?", re.MULTILINE), ""),           # blockquote
]


def _markdown_to_plain_text(markdown_text: str) -> str:
    """Convert a small subset of Markdown to plain text for fpdf2 rendering.

    Args:
        markdown_text: Raw Markdown content.

    Returns:
        Plain text suitable for the PDF core fonts (latin-1 safe).
    """
    text = markdown_text
    for pattern, replacement in _MD_PATTERNS:
        text = pattern.sub(replacement, text)
    # fpdf2 core fonts are latin-1 encoded; replace unsupported characters.
    return text.encode("latin-1", "replace").decode("latin-1")


def export_markdown_to_pdf(md_path: Path | str, pdf_path: Path | str) -> Path:
    """Render a Markdown file to PDF using fpdf2.

    Args:
        md_path: Path to the source ``.md`` file.
        pdf_path: Destination ``.pdf`` path.

    Returns:
        The resolved PDF path.

    Raises:
        ImportError: If ``fpdf2`` is not installed (message includes the
            install command).
        FileNotFoundError: If the Markdown file does not exist.
    """
    try:
        from fpdf import FPDF
    except ImportError as exc:
        raise ImportError(
            "fpdf2 is required for PDF export. Install it with: pip install fpdf2. "
            "Alternatively run the report in Markdown form."
        ) from exc

    md_path, pdf_path = Path(md_path), Path(pdf_path)
    if not md_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {md_path}")

    plain_text = _markdown_to_plain_text(md_path.read_text(encoding="utf-8"))

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "Thunders Report", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5, plain_text)

    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(pdf_path))
    LOGGER.info("PDF exported: %s -> %s", md_path, pdf_path)
    return pdf_path


def main(argv: list[str] | None = None) -> None:
    """Demo entry point: export the latest daily report (or a sample) to PDF."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    daily_reports = sorted(OUTPUT_DIR.glob("daily_*.md"))
    if daily_reports:
        md_path = daily_reports[-1]
    else:
        LOGGER.warning("No daily_*.md found in %s; generating a sample report.", OUTPUT_DIR)
        md_path = OUTPUT_DIR / "sample_report.md"
        md_path.write_text(
            "# Thunders Sample Report\n\n- Rows ingested: 1,234\n- Error rate: 0.50%\n",
            encoding="utf-8",
        )

    pdf_path = md_path.with_suffix(".pdf")
    export_markdown_to_pdf(md_path, pdf_path)
    LOGGER.info("Demo complete; open %s to verify the export.", pdf_path)


if __name__ == "__main__":
    main()
