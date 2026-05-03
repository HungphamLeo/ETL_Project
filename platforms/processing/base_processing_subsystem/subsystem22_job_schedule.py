from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Optional


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _make_id(*parts: str) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def generate_run_id(prefix: str = "run") -> str:
    timestamp = _now_utc().strftime("%Y%m%d_%H%M%S")
    uid = str(uuid.uuid4())[:8]
    return f"{prefix}_{timestamp}_{uid}"


def make_run_id(run_id: Optional[str] = None, prefix: str = "run") -> str:
    return run_id or generate_run_id(prefix)


