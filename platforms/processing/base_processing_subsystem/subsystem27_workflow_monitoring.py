from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class ETLRunRecord:
    """Maps to silver_meta_etl_run Delta table.
    Subsystem 22: Job Scheduler
    Subsystem 27: Workflow Monitoring
    """
    run_id:        str
    job_name:      str
    layer:         str
    table_name:    str
    start_time:    datetime
    end_time:      Optional[datetime] = None
    status:        str = "RUNNING"
    rows_read:     Optional[int] = None
    rows_written:  Optional[int] = None
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["start_time"] = self.start_time.isoformat() if self.start_time else None
        d["end_time"] = self.end_time.isoformat() if self.end_time else None
        d["duration_seconds"] = self.duration_seconds
        return d




