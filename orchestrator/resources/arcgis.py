import json
from typing import Any

import pandas as pd
import requests
from dagster import ConfigurableResource, Failure, get_dagster_logger

logger = get_dagster_logger()

_TIMEOUT = 30


class ArcGISResource(ConfigurableResource):
    """Interact with ArcGIS Online hosted feature layers via the REST API.

    Authenticates using OAuth2 client_credentials (App ID + Secret).
    """

    client_id: str
    client_secret: str
    org_url: str = "https://www.arcgis.com"

    def _get_token(self) -> str:
        url = f"{self.org_url}/sharing/rest/oauth2/token"
        resp = requests.post(
            url,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
                "f": "json",
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            raise Failure(f"ArcGIS token error: {body['error']}")
        return body["access_token"]

    def _query_all_features(self, layer_url: str, token: str, out_fields: list[str]) -> list[dict]:
        """Return all features from the layer with the requested fields + OBJECTID."""
        fields = ",".join({"OBJECTID"} | set(out_fields))
        resp = requests.get(
            f"{layer_url}/query",
            params={
                "where": "1=1",
                "outFields": fields,
                "returnGeometry": "false",
                "f": "json",
                "token": token,
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            raise Failure(f"ArcGIS query error: {body['error']}")
        return [f["attributes"] for f in body.get("features", [])]

    def upsert_features(
        self,
        layer_url: str,
        df: pd.DataFrame,
        key_fields: list[str],
        geometry_field: str | None = None,
    ) -> dict[str, int]:
        """Upsert df rows into the hosted feature layer.

        Matches existing features by key_fields. Rows with no match are added;
        rows with a match are updated (carrying the existing OBJECTID).

        Returns {"adds": n, "updates": n}.
        """
        token = self._get_token()
        existing = self._query_all_features(layer_url, token, key_fields)

        existing_lookup: dict[tuple, int] = {
            tuple(str(row[k]) for k in key_fields): row["OBJECTID"]
            for row in existing
        }

        adds: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []

        for _, row in df.iterrows():
            attrs = {col: _serialize(row[col]) for col in df.columns}
            composite_key = tuple(str(row[k]) for k in key_fields)

            if composite_key in existing_lookup:
                attrs["OBJECTID"] = existing_lookup[composite_key]
                feature: dict[str, Any] = {"attributes": attrs}
                if geometry_field and geometry_field in row:
                    feature["geometry"] = row[geometry_field]
                updates.append(feature)
            else:
                feature = {"attributes": attrs}
                if geometry_field and geometry_field in row:
                    feature["geometry"] = row[geometry_field]
                adds.append(feature)

        resp = requests.post(
            f"{layer_url}/applyEdits",
            data={
                "adds": json.dumps(adds),
                "updates": json.dumps(updates),
                "f": "json",
                "token": token,
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            raise Failure(f"ArcGIS applyEdits error: {body['error']}")

        add_success = sum(1 for r in body.get("addResults", []) if r.get("success"))
        update_success = sum(1 for r in body.get("updateResults", []) if r.get("success"))
        add_fail = len(body.get("addResults", [])) - add_success
        update_fail = len(body.get("updateResults", [])) - update_success

        if add_fail or update_fail:
            logger.warning(f"ArcGIS applyEdits: {add_fail} add failures, {update_fail} update failures")

        logger.info(f"ArcGIS sync: {add_success} added, {update_success} updated")
        return {"adds": add_success, "updates": update_success}

    def replace_features(
        self,
        layer_url: str,
        df: pd.DataFrame,
        geometry_fields: tuple[str, str] | None = None,
    ) -> dict[str, int]:
        """Delete all existing features then add all rows from df.

        geometry_fields: if provided, (lat_col, lon_col) — attaches point geometry.
        Returns {"deleted": n, "added": n}.
        """
        token = self._get_token()

        del_resp = requests.post(
            f"{layer_url}/deleteFeatures",
            data={"where": "1=1", "f": "json", "token": token},
            timeout=_TIMEOUT,
        )
        del_resp.raise_for_status()
        del_body = del_resp.json()
        if "error" in del_body:
            raise Failure(f"ArcGIS deleteFeatures error: {del_body['error']}")
        deleted = len(del_body.get("deleteResults", []))

        geometry_col_set = set(geometry_fields) if geometry_fields else set()
        features: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            attrs = {col: _serialize(row[col]) for col in df.columns if col not in geometry_col_set}
            feature: dict[str, Any] = {"attributes": attrs}
            if geometry_fields:
                lat_col, lon_col = geometry_fields
                feature["geometry"] = {
                    "x": _serialize(row[lon_col]),
                    "y": _serialize(row[lat_col]),
                    "spatialReference": {"wkid": 4326},
                }
            features.append(feature)

        add_resp = requests.post(
            f"{layer_url}/addFeatures",
            data={"features": json.dumps(features), "f": "json", "token": token},
            timeout=_TIMEOUT,
        )
        add_resp.raise_for_status()
        add_body = add_resp.json()
        if "error" in add_body:
            raise Failure(f"ArcGIS addFeatures error: {add_body['error']}")

        added = sum(1 for r in add_body.get("addResults", []) if r.get("success"))
        add_fail = len(add_body.get("addResults", [])) - added
        if add_fail:
            logger.warning(f"ArcGIS addFeatures: {add_fail} failures")

        logger.info(f"ArcGIS replace: {deleted} deleted, {added} added")
        return {"deleted": deleted, "added": added}


def _serialize(value: Any) -> Any:
    """Convert pandas/numpy scalar types to JSON-serializable Python types."""
    if pd.isna(value) if not isinstance(value, (list, dict)) else False:
        return None
    if hasattr(value, "item"):
        return value.item()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
