"""
Reconciliation Engine module shim.
Enables direct importing of `reconciliation_engine` from both workspace root and backend directory.
"""
import sys
from pathlib import Path

_current_dir = Path(__file__).resolve().parent
_rec_dir = _current_dir / "reconciliation"
for _p in (str(_current_dir), str(_rec_dir)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from reconciliation.reconciliation_engine import ReconciliationEngine

__all__ = ["ReconciliationEngine"]
