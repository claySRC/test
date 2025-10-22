"""
tooklkit.apis.gpm module

Example usage:

import pandas as pd
from toolkit.apis.gpm import GpmClient
gpm = GpmClient()

df = pd.DataFrame(
    gpm.get(
        "/DataList/v2",
        headers={"TimeZone":"UTC"},
        params={
            "dataSourceIds": "2764301",
            "startDate": "2025-05-01",
            "endDate": "2025-05-02",
            "grouping": "raw"
        }
    )
    .json()
)
"""
import logging
import requests
import pandas as pd
from datetime import datetime
from toolkit.keyvault import Vault
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logger = logging.getLogger("GpmClient")
logger.setLevel(logging.DEBUG)  # Set logging level specifically for GpmClient
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(handler)

def _keyValueList_to_table(kv_list):
    """Helper to display a list of key-value dictionaries as an HTML table"""
    record = {}
    for dict_item in kv_list:
        record[dict_item['Key']] = dict_item['Value']
        
    return record

class GpmClient:
    """
    Helper class for GPM+ API.
    """

    def __init__(self, config_path = None, gpm_plus_server_name="siliconranch"):
        self.config_path = config_path
        self.gpm_plus_server_name = gpm_plus_server_name
        self.url_api = f"https://webapi{self.gpm_plus_server_name}.horizon.greenpowermonitor.com/api"
        self.auth_protocol = ""
        self.gpm_username, self.gpm_password = self._load_credentials()
        self.auth_protocol = self._request_token(f"{self.url_api}/Account/Token")

    def _load_credentials(self):
        try:
            kv = Vault(self.config_path)
            username = kv.get_secret("gpm-user")
            password = kv.get_secret("gpm-pass")
            logger.info("Using GPM credentials from Key Vault.")
        except Exception:
            raise ValueError(
                "Unable to fetch GPM credentials from Key Vault. Checking environment variables."
            )

        if not username or not password:
            raise ValueError("GPM credentials are missing.")
        return username, password

    def _request_token(self, api_url):
        response = requests.post(
            api_url, json={"username": self.gpm_username, "password": self.gpm_password}
        )
        if response.status_code != 200:
            logger.error(f"Failed to get token: {response.text}")
            raise ValueError(f"Failed to get token from {api_url}")

        data = response.json()
        token = f"Bearer {data['AccessToken']}"
        logger.info("Token successfully retrieved.")
        return token

    def request(
        self,
        endpoint,
        method="get",
        data=None,
        params=None,
        headers=None,
        timeout=None,
        auth=None,
        **kwargs,
    ):
        url = f"{self.url_api}{endpoint}"
        default_headers = {
            "Authorization": self.auth_protocol,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if headers:
            default_headers.update(headers)
        
        response = requests.request(
            method,
            url,
            headers=default_headers,
            json=data,
            params=params,
            timeout=timeout,
            auth=auth,
            **kwargs,
        )

        return response

    def get(
        self, endpoint, params=None, headers=None, timeout=None, auth=None, **kwargs
    ):
        """
        Convenience method for GET requests.
        """
        return self.request(
            endpoint,
            method="get",
            params=params,
            headers=headers,
            timeout=timeout,
            auth=auth,
            **kwargs,
        )

    def post(
        self, endpoint, data=None, headers=None, timeout=None, auth=None, **kwargs
    ):
        """
        Convenience method for POST requests.
        """
        return self.request(
            endpoint,
            method="post",
            data=data,
            headers=headers,
            timeout=timeout,
            auth=auth,
            **kwargs,
        )

    def put(self, endpoint, data=None, headers=None, timeout=None, auth=None, **kwargs):
        """
        Convenience method for PUT requests.
        """
        return self.request(
            endpoint,
            method="put",
            data=data,
            headers=headers,
            timeout=timeout,
            auth=auth,
            **kwargs,
        )

    def data_list(
        self, data_source_id, start_date, end_date, aggreg_type=1, grouping="raw"
    ):
        params = {
            "datasourceId": data_source_id,
            "startDate": datetime(*start_date).timestamp(),
            "endDate": datetime(*end_date).timestamp(),
            "aggregationType": aggreg_type,
            "grouping": grouping,
        }
        response = self.request("/DataList", params=params)
        
        return response

    def data_list_v2(
        self,
        data_source_ids,
        start_date_str,
        end_date_str,
        aggreg_type=1,
        grouping="raw",
        additional_params=None,
    ):
        """
        Helper function for the /DataList/v2 endpoint.
        """
        params = {
            "datasourceIds": data_source_ids,
            "startDate": start_date_str,
            "endDate": end_date_str,
            "aggregationType": aggreg_type,
            "grouping": grouping,
        }
        if additional_params:
            params.update(additional_params)

        response = self.request("/DataList/v2", params=params)

        return response
    
    def data_list_v2_parallel(
        self,
        ds_ids,
        start,
        end,
        *,
        batch_size=1,
        max_workers=8,
        grouping="raw",
        aggreg_type=1,
        headers=None,
        additional_params=None,
        timeout=None,
        as_dataframe=True,
        tz_local=False
    ):
        """
        Parallel batched fetch for /DataList/v2.

        ds_ids         Iterable of datasource ids.
        start/end      ISO8601 strings or datetime objects accepted by .isoformat().
        batch_size     Max datasourceIds per request.
        max_workers    Max parallel requests.
        headers        Extra headers; TimeZone is set to UTC by default if not provided.
        additional_params  Dict to extend query params.
        as_dataframe   If True, returns a single concatenated pandas DataFrame; else list of raw payloads.
        """
        # Local helpers to keep this method self-contained.
        def _iso(dt):
            return dt if isinstance(dt, str) else dt.isoformat(timespec="seconds")

        def _chunk(seq, size):
            for i in range(0, len(seq), size):
                yield seq[i:i + size]

        tz_headers = {"TimeZone": "UTC"} if tz_local is False else {}
        if headers:
            tz_headers.update(headers)

        start_iso, end_iso = _iso(start), _iso(end)
        ds_ids = list(ds_ids)
        if not ds_ids:
            return pd.DataFrame() if as_dataframe else []

        def _submit(batch):
            params = {
                "dataSourceIds": ",".join(map(str, batch)),
                "startDate": start_iso,
                "endDate": end_iso,
                "aggregationType": aggreg_type,
                "grouping": grouping,
            }
            if additional_params:
                params.update(additional_params)
            # Reuse existing get() path; keeps auth and base URL behavior unchanged.
            return self.get("/DataList/v2", params=params, headers=tz_headers, timeout=timeout)

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_submit, batch): batch for batch in _chunk(ds_ids, batch_size)}
            for fut in as_completed(futures):
                try:
                    resp = fut.result()
                    resp.raise_for_status()
                    payload = resp.json()
                except Exception as e:
                    logger.error(f"/DataList/v2 failed for batch starting {futures[fut][0]}: {e}")
                    payload = []
                results.append(payload)

        if not as_dataframe:
            # Return list of payloads as-is.
            return results

        # Normalize to tidy DataFrame consistent with GpmSync expectations.
        parts = []
        for payload in results:
            if not payload:
                continue
            df = pd.DataFrame(payload)
            if not df.empty:
                ts_col = "timestamp_local" if tz_local else "timestamp_utc"
                df = df.rename(columns={"DataSourceId": "datasource_id", "Date": ts_col, "Value": "value"})
                if ts_col in df.columns:
                    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
            parts.append(df)
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    def plantsdf(self):
        """
        Get a DataFrame of all plants in GPM.
        """
        response = self.get('/Plant')
        response.raise_for_status()
        _plants = pd.DataFrame(response.json())
        _plants['Properties'] = _plants['Parameters'].apply(lambda x: _keyValueList_to_table(x))

        plants = _plants[[
        'Id',
        'Name',
        'ElementCount',
        'UniqueID',
        'Properties'
        ]].to_dict('records')

        wf_properties = [ {"Id":p['Id'], "Name":p['Name'], "ElementCount":p['ElementCount'], 'UniqueID':p['UniqueID'], **p['Properties']} for p in plants]
        
        return pd.DataFrame(wf_properties)
