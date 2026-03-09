"""
learning_analytics_overview.py

Compatibility entrypoint that forwards to the main Streamlit overview app.

Dependencies
------------
- pathlib
- streamlit_app
- sys

Classes
-------
- None.

Functions
---------
- None.
"""
from __future__ import annotations

import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from streamlit_app import main

if __name__ == "__main__":
    main()
