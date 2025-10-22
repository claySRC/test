# GPM → Power BI API (FastAPI)

This tiny service wraps your existing `GpmClient` so Power BI (Service) can call simple JSON endpoints.

## Endpoints

- `GET /health`
- `GET /plants`
- `GET /elements?plant_id=17`
- `GET /tags?plant_id=17&element_id=2774`
- `GET /data?data_source_ids=123,456&start=2025-10-01T00:00:00&end=2025-10-02T00:00:00&tz=UTC`

## Local Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill GPM_USER/GPM_PASS
uvicorn app:app --reload --port 8080
```

## Docker

```bash
docker build -t gpm-powerbi-api .
docker run -it --rm -p 8080:8080 --env-file .env gpm-powerbi-api
```

## Deploy (Azure Container Apps quick outline)

1) Push image to ACR
```
az acr create -n <acr_name> -g <rg> --sku Basic
az acr build -t gpm-powerbi-api:latest -r <acr_name> .
```

2) Create Container App (HTTP ingress)
```
az containerapp create -n gpm-powerbi-api -g <rg>   --environment <env_name>   --image <acr_name>.azurecr.io/gpm-powerbi-api:latest   --ingress external --target-port 8080   --env-vars GPM_USER=... GPM_PASS=... GPM_PLUS_SERVER_NAME=siliconranch
```

Copy the FQDN it prints (e.g., `https://gpm-powerbi-api.<region>.azurecontainerapps.io`).

## Connect from Power BI Service

- New dataset → **Get Data → Web**
- URL: `https://<fqdn>/plants` (JSON)
- Expand columns, save dataset.
- Repeat for `/elements`, `/tags`, `/data`.
