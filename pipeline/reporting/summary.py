from collections import defaultdict
from statistics import median

from config_loader import RunConfig
from reporting.models import QueryRow


def _statement_medians(query_records: list[QueryRow]) -> dict[tuple[str, int], float]:
    """
    Median seconds of the successful runs of each (query file, statement part).
    Returns a dictionary mapping (query_filename, statement_part) to median
    seconds.
    """
    run_seconds = defaultdict(list)
    for record in query_records:
        if record.status == "success":
            run_seconds[(record.query_filename, record.statement_part)].append(
                record.seconds
            )
    return {
        statement: median(seconds) for statement, seconds in run_seconds.items()
    }


def build_summary(
    query_records: list[QueryRow],
    run_id: str,
    started_at: str,
    scale_factor: int,
    query_selection: str,
    parquet_dir: str,
    queries_dir: str,
    options: RunConfig,
) -> dict:
    """
    Aggregate the per-statement records into a single run summary row.
    """
    runs_per_query = options.general.runs_per_query
    statement_medians = _statement_medians(query_records)
    statements = {(r.query_filename, r.statement_part) for r in query_records}

    return {
        "run_id": run_id,
        "started_at": started_at,
        "scale_factor": scale_factor,
        "query_selection": query_selection,
        "parquet_dir": parquet_dir,
        "queries_dir": queries_dir,
        "runs_per_query": runs_per_query,
        "total_seconds": sum(statement_medians.values()),
        # A statement succeeded if at least one of its runs did, i.e. exactly the
        # statements counted in total_seconds
        "statements_succeeded": len(statement_medians),
        "statements_failed": len(statements) - len(statement_medians),
    }
