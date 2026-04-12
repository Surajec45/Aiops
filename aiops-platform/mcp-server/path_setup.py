"""Ensure aiops-platform root is on sys.path so `shared.*` imports work when running from mcp-server/.

In Docker, `shared` is copied next to this file under /app; PYTHONPATH=/app is enough — skip extra path hacks.
"""

import sys
from pathlib import Path


def ensure_platform_path() -> None:
    here = Path(__file__).resolve().parent
    if (here / "shared").is_dir():
        return
    platform_root = here.parent
    s = str(platform_root)
    if s not in sys.path:
        sys.path.insert(0, s)
