from pathlib import Path
import subprocess

SCRIPT_DIR = Path(__file__).parent
PDFTOTEXT = SCRIPT_DIR / "poppler" / "Library" / "bin" / "pdftotext.exe"

assert PDFTOTEXT.exists(), f"pdftotext not found: {PDFTOTEXT}"

for pdf in SCRIPT_DIR.glob("*.pdf"):
    txt = pdf.with_suffix(".txt")
    print(f"{pdf.name} -> {txt.name}")
    subprocess.run([str(PDFTOTEXT), "-layout", str(pdf), str(txt)], check=True)

