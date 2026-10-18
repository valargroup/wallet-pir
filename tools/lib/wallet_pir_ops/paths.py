"""Resource root shared by a source checkout and an unpacked operations release."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
# Product packages live with their product code, in both layouts.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
