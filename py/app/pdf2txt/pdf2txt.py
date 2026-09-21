from pathlib import Path
import shutil
import subprocess

PDFTOTEXT = shutil.which("pdftotext")
assert PDFTOTEXT, "pdftotext not found on PATH (install poppler-utils)"

SCRIPT_DIR = Path(__file__).parent

for pdf in SCRIPT_DIR.glob("*.pdf"):
    txt = pdf.with_suffix(".txt")
    print(f"{pdf.name} -> {txt.name}")
    subprocess.run([PDFTOTEXT, "-layout", str(pdf), str(txt)], check=True)
