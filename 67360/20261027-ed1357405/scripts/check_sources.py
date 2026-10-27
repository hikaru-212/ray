"""New packaging helper; not a producer of the archived runtime results."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
provenance = json.loads((root / "provenance.json").read_text())
checked = 0
for variant, files in provenance["critical_source_files"].items():
    for relative, expected in files.items():
        path = root / "sources" / variant / relative
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected["sha256"]:
            raise SystemExit(f"Source hash mismatch: {variant}/{relative}")
        checked += 1
print(f"Verified {checked} selected source files; this is not the full archived source audit.")
