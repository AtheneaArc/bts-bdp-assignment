from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query
from pydantic import BaseModel

from bdi_api.settings import Settings

from pymongo import MongoClient
from fastapi import HTTPException

settings = Settings()

s6 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s6",
    tags=["s6"],
)


class AircraftPosition(BaseModel):
    icao: str
    registration: str | None = None
    type: str | None = None
    lat: float
    lon: float
    alt_baro: float | None = None
    ground_speed: float | None = None
    timestamp: str


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB.

    Use the BDI_MONGO_URL environment variable to configure the connection.
    Start MongoDB with: make mongo
    Database name: bdi_aircraft
    Collection name: positions
    """
    # TODO: Connect to MongoDB using pymongo.MongoClient(settings.mongo_url)
    client= MongoClient(settings.mongo_url)
    db = client["bdi_aircraft"]
    # TODO: Insert the position document into the 'positions' collection
    collection=db["positions"]
    collection.insert_one(position.model_dump())
    # TODO: Return {"status": "ok"}
    return {"status": "ok"}


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type.

    Response example: [{"type": "B738", "count": 42}, {"type": "A320", "count": 38}]

    Use MongoDB's aggregation pipeline with $group.
    """
    # TODO: Connect to MongoDB
    client= MongoClient(settings.mongo_url)
    db = client["bdi_aircraft"]
    # TODO: Use collection.aggregate() with $group on 'type' field
    collection=db["positions"]
    pipeline=[{"$group": {"_id": "$type", "count": {"$sum": 1}}},{"$sort": {"count": -1}}]
    results = collection.aggregate(pipeline)
    # TODO: Return list sorted by count descending
    return[{"type": r["_id"], "count": r["count"]} for r in results]


@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    page_size: Annotated[
        int,
        Query(description="Number of results per page", ge=1, le=100),
    ] = 20,
) -> list[dict]:
    """List all aircraft with pagination.

    Each result should include: icao, registration, type.
    Use MongoDB's skip() and limit() for pagination.
    """
    # TODO: Connect to MongoDB
    client= MongoClient(settings.mongo_url)
    db = client["bdi_aircraft"]
    collection = db["positions"]
    # TODO: Query distinct aircraft, apply skip/limit for pagination
    skip = (page - 1) * page_size
    cursor=collection.find({}, {"_id":0,"icao":1,"registration":1,"type":1}).skip(skip).limit(page_size)
    # TODO: Return list of dicts with icao, registration, type
    return list(cursor)


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position data for a specific aircraft.

    Return the most recent document matching the given ICAO code.
    If not found, return 404.
    """
    # TODO: Connect to MongoDB
    client= MongoClient(settings.mongo_url)
    db = client["bdi_aircraft"]
    collection = db["positions"]
    # TODO: Find the latest document for this icao (sort by timestamp descending)
    doc=collection.find_one({"icao":icao},sort=[("timestamp",-1)],projection={"_id":0})
    # TODO: Return 404 if not found
    if not doc:
        raise HTTPException(status_code=404, detail="Aircraft not found")
    return doc


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    # TODO: Connect to MongoDB
    client= MongoClient(settings.mongo_url)
    db = client["bdi_aircraft"]
    collection = db["positions"]
    # TODO: Delete all documents matching the icao
    result = collection.delete_many({"icao": icao})
    # TODO: Return {"deleted": <count>}
    return {"deleted": result.deleted_count}
