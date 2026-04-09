import json
import os

import pandas as pd
from fastapi import APIRouter, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


def _load_tracking_df() -> pd.DataFrame:
    path = settings.prepared_dir
    if not os.path.isdir(path):
        return pd.DataFrame()

    files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(".parquet")]
    if not files:
        return pd.DataFrame()

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    if "hex" in df.columns:
        df = df.rename(columns={"hex": "icao"})
    return df


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all unique aircraft with enriched data, ordered by ICAO ascending."""
    df = _load_tracking_df()
    if df.empty:
        return []

    # One row per aircraft — keep first occurrence which carries enrichment columns
    df = df.drop_duplicates(subset=["icao"])
    df = df.sort_values("icao")

    start = page * num_results
    df_page = df.iloc[start : start + num_results]

    def clean(val):
        return None if pd.isna(val) else val

    return [
        AircraftReturn(
            icao=row["icao"],
            registration=clean(row.get("registration")),
            type=clean(row.get("type")),
            owner=clean(row.get("owner")),
            manufacturer=clean(row.get("manufacturer")),
            model=clean(row.get("model")),
        )
        for _, row in df_page.iterrows()
    ]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    """Calculate CO2 emissions for a given aircraft on a specific day."""
    df = _load_tracking_df()
    if df.empty:
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)

    df_filtered = df[df["icao"] == icao]
    if "day" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["day"] == day]

    count = len(df_filtered)
    hours_flown = round((count * 5) / 3600, 2)

    if df_filtered.empty:
        return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=None)

    aircraft_type = df_filtered.iloc[0].get("type")
    if pd.isna(aircraft_type) if aircraft_type is not None else True:
        return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=None)
    aircraft_type = str(aircraft_type)

    fuel_rates_path = os.path.join(settings.local_dir, "fuel_rates.json")
    if not os.path.isfile(fuel_rates_path):
        return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=None)

    with open(fuel_rates_path) as f:
        fuel_data = json.load(f)

    if aircraft_type not in fuel_data:
        return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=None)

    galph = fuel_data[aircraft_type]["galph"]
    fuel_used_kg = hours_flown * galph * 3.04
    co2_tons = round((fuel_used_kg * 3.15) / 907.185, 2)

    return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=co2_tons)
