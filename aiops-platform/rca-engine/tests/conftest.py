import sys
from pathlib import Path

# aiops-platform (for shared.*) and rca-engine (for schemas, utils, graph)
_platform = Path(__file__).resolve().parent.parent.parent
_rca = Path(__file__).resolve().parent.parent
for _p in (_platform, _rca):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)
