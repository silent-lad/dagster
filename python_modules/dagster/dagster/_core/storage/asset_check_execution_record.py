import enum
from typing import NamedTuple, Optional

from dagster import EventLogEntry


class AssetCheckExecutionStatus(enum.Enum):
    PLANNED = "PLANNED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"  # explicit fail result
    RUN_FAILURE = "RUN_FAILURE"  # hit some exception
    SKIPPED = "SKIPPED"  # the run finished, didn't fail, but the check didn't execute


class AssetCheckExecutionRecord(NamedTuple):
    id: int
    run_id: str
    status: AssetCheckExecutionStatus
    evaluation_event: Optional[EventLogEntry]
    create_timestamp: float
