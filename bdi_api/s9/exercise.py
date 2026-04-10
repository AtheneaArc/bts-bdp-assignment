import json
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

s9 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s9",
    tags=["s9"],
)

_DATA_FILE = Path(__file__).parent / "pipelines_data.json"


def _load_pipelines() -> list[dict]:
    with open(_DATA_FILE) as f:
        return json.load(f)


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: datetime | None
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    logs_url: str


@s9.get("/pipelines")
def list_pipelines(
    repository: str | None = None,
    status_filter: str | None = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    """List CI/CD pipeline runs with their status.

    Returns a list of pipeline runs, optionally filtered by repository and status.
    Ordered by started_at descending (most recent first).
    Paginated with `num_results` per page and `page` number (0-indexed).

    Valid statuses: "success", "failure", "running", "pending"
    Valid triggered_by values: "push", "pull_request", "schedule", "manual"
    """
    pipelines = _load_pipelines()

    if repository is not None:
        pipelines = [p for p in pipelines if p["repository"] == repository]

    if status_filter is not None:
        pipelines = [p for p in pipelines if p["status"] == status_filter]

    pipelines.sort(key=lambda p: p["started_at"], reverse=True)

    start = page * num_results
    pipelines = pipelines[start : start + num_results]

    return [PipelineRun(**{k: v for k, v in p.items() if k != "stage_details"}) for p in pipelines]


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    """Get the stages of a specific pipeline run.

    Returns the stages in execution order.
    Each stage has a name, status, timestamps, and a logs URL.

    Typical stages: "lint", "test", "build", "deploy"
    """
    pipelines = _load_pipelines()
    pipeline = next((p for p in pipelines if p["id"] == pipeline_id), None)

    if pipeline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pipeline '{pipeline_id}' not found")

    return [PipelineStage(**stage) for stage in pipeline["stage_details"]]
