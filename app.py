
from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from datetime import datetime, timedelta
import os
import pandas as pd

# Import the user's GpmClient
from gpm import GpmClient

# ---- Shim: subclass to support ENV fallback if Vault isn't available ----
class GpmClientEnv(GpmClient):
    def _load_credentials(self):
        # Try standard Vault path first (super) and then fallback to ENV
        try:
            return super()._load_credentials()
        except Exception:
            user = os.getenv("GPM_USER")
            pw = os.getenv("GPM_PASS")
            if not user or not pw:
                raise ValueError(
                    "GPM credentials missing. Set env vars GPM_USER and GPM_PASS."
                )
            return user, pw

# Factory for client using ENV for server name, optional config_path
def get_client():
    server = os.getenv("GPM_PLUS_SERVER_NAME", "siliconranch")
    config_path = os.getenv("GPM_CONFIG_PATH", None)
    return GpmClientEnv(config_path=config_path, gpm_plus_server_name=server)

app = FastAPI(title="GPM → Power BI API", version="1.0.0")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/plants")
def plants():
    try:
        gpm = get_client()
        df = gpm.plantsdf()
        # Convert nested objects to plain JSON-safe dict/list
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/elements")
def elements(plant_id: int = Query(..., description="GPM Plant Id")):
    try:
        gpm = get_client()
        r = gpm.get(f"/Plant/{plant_id}/Element")
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tags")
def tags(plant_id: int, element_id: int):
    try:
        gpm = get_client()
        r = gpm.get(f"/Plant/{plant_id}/Element/{element_id}/Datasource")
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data")
def data(
    data_source_ids: str = Query(..., description="Comma-separated DataSourceId(s)"),
    start: str = Query(..., description="ISO8601 start (e.g., 2025-10-01T00:00:00)"),
    end: str = Query(..., description="ISO8601 end (e.g., 2025-10-02T00:00:00)"),
    tz: Optional[str] = Query("UTC", description="TimeZone header (UTC or Local)"),
    grouping: Optional[str] = Query("raw", description="raw or interval grouping"),
    aggregationType: Optional[int] = Query(1, description="Aggregation type"),
):
    try:
        gpm = get_client()
        addl = {"aggregationType": aggregationType, "grouping": grouping}
        headers = {"TimeZone": "UTC"} if tz and tz.upper()=="UTC" else {}
        resp = gpm.get(
            "/DataList/v2",
            params={
                "dataSourceIds": data_source_ids,
                "startDate": start,
                "endDate": end,
                "grouping": grouping,
                "aggregationType": aggregationType
            },
            headers=headers
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
