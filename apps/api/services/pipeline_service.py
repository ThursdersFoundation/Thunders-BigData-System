"""Business logic for pipelines."""


async def trigger_run(pipeline_id: str) -> dict:
    return {"run_id": "TODO", "pipeline_id": pipeline_id, "status": "queued"}
