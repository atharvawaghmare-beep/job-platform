"""Pytest sys.path setup.

Being in `api/` gets this directory itself prepended to sys.path
automatically (pytest's default behavior for a conftest.py with no
`__init__.py` alongside it), which is what lets tests do
`from app.main import app`.

That alone isn't enough for `from common.models import ...`, though:
inside Docker, common/ is copied in as a sibling of app/ (both under /app),
but on the host, common/ actually lives one level up at the repo root. Add
it explicitly so a plain `pytest` run from api/ works the same both places,
with no PYTHONPATH env var required.
"""
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
