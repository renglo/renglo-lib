"""
Lightweight docs controller used by extension handlers.

Historically handlers wired a DocsController alongside DataController.
Most current handlers only assign ``self.DCC`` and do not call it yet.
This module exists so handler imports succeed; expand when doc-specific
APIs are needed.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from renglo.logger import get_logger


class DocsController:
    def __init__(self, config: Optional[Dict[str, Any]] = None, tid=None, ip=None):
        self.config = config or {}
        self.tid = tid
        self.ip = ip
        self.logger = get_logger()
