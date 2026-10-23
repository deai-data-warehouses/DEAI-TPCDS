def build_summary(
    query_records: list[dict],
    run_id: str,
    started_at: str,
    scale_factor: int,
    runs_per_query: int,
    parquet_dir: str,
    queries_dir: str,
    run_elapsed_seconds: float,
) -> dict:
    """Aggregate the per-statement records into a single run summary row."""
    successful_records = [r for r in query_records if r["status"] == "success"]
    all_query_times = [r["elapsed_seconds"] for r in query_records]
    successful_query_times = [r["elapsed_seconds"] for r in successful_records]

    return {
        "run_id": run_id,
        "started_at": started_at,
        "scale_factor": scale_factor,
        "runs_per_query": runs_per_query,
        "parquet_dir": parquet_dir,
        "queries_dir": queries_dir,
        "run_elapsed_seconds": run_elapsed_seconds,
        "total_query_seconds": sum(all_query_times),
        "successful_query_seconds": sum(successful_query_times),
        "average_statement_seconds": (
            sum(successful_query_times) / len(successful_query_times)
            if successful_query_times
            else None
        ),
        "attempted_statements": len(query_records),
        "successful_statements": len(successful_records),
        "failed_statements": len(query_records) - len(successful_records),
    }
