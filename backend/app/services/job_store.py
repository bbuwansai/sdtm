from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List, Optional

from app.config import JOBS_DIR
from app.utils.io import read_json, write_json


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class TimelineEvent:
    time: str
    level: str
    message: str


@dataclass
class JobRecord:
    job_id: str
    status: str = "queued"
    filename: Optional[str] = None
    domain: Optional[str] = None
    domain_confidence: Optional[float] = None
    current_step: Optional[str] = None
    error: Optional[str] = None
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    timeline: List[TimelineEvent] = field(default_factory=list)
    artifacts: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, str] = field(default_factory=dict)

    def to_dict(self):
        payload = asdict(self)
        payload["timeline"] = [asdict(item) for item in self.timeline]
        return payload


class JobStore:
    def __init__(self) -> None:
        self.base = JOBS_DIR
        self.base.mkdir(parents=True, exist_ok=True)

    def path(self, job_id: str) -> Path:
        return self.base / job_id / "job.json"

    def load(self, job_id: str) -> JobRecord:
        raw = read_json(self.path(job_id))
        if raw is None:
            raise FileNotFoundError(job_id)
        raw["timeline"] = [TimelineEvent(**item) for item in raw.get("timeline", [])]
        return JobRecord(**raw)

    def save(self, record: JobRecord) -> None:
        record.updated_at = utc_now()
        write_json(self.path(record.job_id), record.to_dict())

    def create(self, job_id: str, filename: str, domain: Optional[str] = None) -> JobRecord:
        record = JobRecord(job_id=job_id, filename=filename, domain=domain)
        self.save(record)
        return record

    def append(self, job_id: str, message: str, level: str = "info", step: Optional[str] = None) -> None:
        record = self.load(job_id)
        if step:
          record.current_step = step
        record.timeline.append(TimelineEvent(time=utc_now(), level=level, message=message))
        self.save(record)

    def patch(self, job_id: str, **kwargs) -> JobRecord:
        record = self.load(job_id)
        for key, value in kwargs.items():
            setattr(record, key, value)
        self.save(record)
        return record
