from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class QueryRow(BaseModel):
    """Result of a single run of a single query statement."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str
    query_filename: str
    query_number: int = Field(gt=0)
    query_version: str
    statement_part: int = Field(default=1, gt=0)  # 1-based, for multi-statement files
    run_number: int = Field(gt=0)  # 1-based, up to runs_per_query
    seconds: float | None = Field(default=None, ge=0)  # None if the run failed
    status: Literal["success", "error"]
    row_count: int | None = Field(default=None, ge=0)  # None if the run failed
    result_check: str = ""  # toolkit/results.compare_results status, first run only
    error: str = ""
