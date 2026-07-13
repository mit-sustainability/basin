import json
from typing import Any

import pandas as pd
import requests
from dagster import ConfigurableResource, Failure, get_dagster_logger
from pydantic import field_validator

logger = get_dagster_logger()

_AUTH_TIMEOUT = 30
_DATA_TIMEOUT = 120


class ArcGISResource(ConfigurableResource):
    """Interact with ArcGIS Online hosted feature layers via the REST API.

    Authenticates using OAuth2 client_credentials (App ID + Secret).
    """

    client_id: str
    client_secret: str
    org_url: str = "https://www.arcgis.com"

    @field_validator("org_url")
    @classmethod
    def _strip_trailing_slash(cls, v: str) -> str:
        return v.rstrip("/")

    def _get_token(self) -> str:
        if not self.client_id or not self.client_secret:
            raise Failure(
                "ArcGISResource is missing client_id/client_secret. "
                "Set ARCGIS_CLIENT_ID and ARCGIS_CLIENT_SECRET."
            )
        url = f"{self.org_url}/sharing/rest/oauth2/token"
        resp = requests.post(
            url,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
                "f": "json",
            },
            timeout=_AUTH_TIMEOUT,
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
            timeout=_DATA_TIMEOUT,
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
            timeout=_DATA_TIMEOUT,
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

    # Maps pandas dtype kinds to ArcGIS field type strings.
    _DTYPE_TO_ESRI = {
        "f": "esriFieldTypeDouble",
        "i": "esriFieldTypeInteger",
        "u": "esriFieldTypeInteger",
        "b": "esriFieldTypeSmallInteger",
        "O": "esriFieldTypeString",
        "U": "esriFieldTypeString",
        "M": "esriFieldTypeString",  # ponytail: datetimes sent as strings anyway
    }
    _SYSTEM_FIELD_TYPES = {
        "esriFieldTypeOID",
        "esriFieldTypeGlobalID",
        "esriFieldTypeGeometry",
    }

    def _ensure_fields(self, layer_url: str, df: pd.DataFrame, token: str, skip_cols: set[str]) -> None:
        """Add any DataFrame columns missing from the layer schema via addToDefinition.

        Safe to call repeatedly — only pushes fields that don't already exist.
        """
        info = requests.get(layer_url, params={"f": "json", "token": token}, timeout=_DATA_TIMEOUT)
        info.raise_for_status()
        layer_info = info.json()

        existing = {
            f["name"]
            for f in layer_info.get("fields", [])
            if f.get("type") not in self._SYSTEM_FIELD_TYPES
        }

        new_fields = [
            {
                "name": col,
                "alias": col,
                "type": self._DTYPE_TO_ESRI.get(df[col].dtype.kind, "esriFieldTypeString"),
                "length": 255 if df[col].dtype.kind in ("O", "U", "M") else None,
                "nullable": True,
                "editable": True,
            }
            for col in df.columns
            if col not in existing and col not in skip_cols
        ]

        if not new_fields:
            return

        # Strip None length — ArcGIS rejects it for numeric types.
        for f in new_fields:
            if f["length"] is None:
                del f["length"]

        logger.info(f"ArcGIS schema: adding fields: {[f['name'] for f in new_fields]}")
        # ponytail: token must be a URL param here — addToDefinition is an admin op;
        # putting it in the POST body produces a spurious 400 "Invalid query parameters".
        resp = requests.post(
            f"{layer_url}/addToDefinition",
            params={"f": "json", "token": token},
            data={"addToDefinition": json.dumps({"fields": new_fields})},
            timeout=_DATA_TIMEOUT,
        )
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            # addToDefinition requires an owner/admin token; client_credentials tokens can't do this.
            # Log and continue — addFeatures will still work once fields exist in the layer.
            logger.warning(
                f"ArcGIS addToDefinition failed (likely token permission): {body['error']}\n"
                f"Add these fields manually in AGOL → Data → Fields → + Add field: "
                f"{[f['name'] for f in new_fields]}"
            )
            return

        logger.info(f"ArcGIS schema: added {len(new_fields)} field(s): {[f['name'] for f in new_fields]}")

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

        # Check layer capabilities before attempting writes.
        cap_resp = requests.get(layer_url, params={"f": "json", "token": token}, timeout=_DATA_TIMEOUT)
        cap_resp.raise_for_status()
        layer_meta = cap_resp.json()
        capabilities = layer_meta.get("capabilities", "")
        if "Delete" not in capabilities or "Create" not in capabilities:
            raise Failure(
                f"Layer does not support editing (capabilities: {capabilities!r}). "
                "Enable editing (Add + Update + Delete) in ArcGIS Online → Content → Settings."
            )

        skip = set(geometry_fields) if geometry_fields else set()
        self._ensure_fields(layer_url, df, token, skip_cols=skip)

        # Fetch all ObjectIDs first (light query), then delete in batches.
        # deleteFeatures where=1=1 causes 504 on large layers; truncateFeatures
        # requires Sync capability not enabled on new UI-created layers.
        ids_resp = requests.get(
            f"{layer_url}/query",
            params={"where": "1=1", "returnIdsOnly": "true", "f": "json", "token": token},
            timeout=_DATA_TIMEOUT,
        )
        ids_resp.raise_for_status()
        ids_body = ids_resp.json()
        if "error" in ids_body:
            raise Failure(f"ArcGIS query (ids) error: {ids_body['error']}")
        object_ids: list[int] = ids_body.get("objectIds") or []

        deleted = 0
        _DELETE_BATCH = 500
        for i in range(0, len(object_ids), _DELETE_BATCH):
            batch_ids = object_ids[i : i + _DELETE_BATCH]
            del_resp = requests.post(
                f"{layer_url}/deleteFeatures",
                data={"objectIds": ",".join(str(oid) for oid in batch_ids), "f": "json", "token": token},
                timeout=_DATA_TIMEOUT,
            )
            del_resp.raise_for_status()
            del_body = del_resp.json()
            if "error" in del_body:
                raise Failure(f"ArcGIS deleteFeatures error: {del_body['error']}")
            deleted += len(del_body.get("deleteResults", []))

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

        added = 0
        batch_size = 1000
        for i in range(0, max(len(features), 1), batch_size):
            batch = features[i : i + batch_size]
            if not batch:
                break
            add_resp = requests.post(
                f"{layer_url}/addFeatures",
                data={"features": json.dumps(batch), "f": "json", "token": token},
                timeout=_DATA_TIMEOUT,
            )
            add_resp.raise_for_status()
            add_body = add_resp.json()
            if "error" in add_body:
                raise Failure(f"ArcGIS addFeatures error: {add_body['error']}")

            batch_success = sum(1 for r in add_body.get("addResults", []) if r.get("success"))
            batch_fail = len(add_body.get("addResults", [])) - batch_success
            if batch_fail:
                raise Failure(
                    f"ArcGIS addFeatures: {batch_fail} record(s) failed in batch {i // batch_size + 1}"
                )
            added += batch_success

        logger.info(f"ArcGIS replace: {deleted} deleted, {added} added")
        return {"deleted": deleted, "added": added}


def _serialize(value: Any) -> Any:
    """Convert pandas/numpy scalar types to JSON-serializable Python types."""
    if pd.isna(value) if not isinstance(value, (list, dict)) else False:
        return None
    if isinstance(value, bool):
        return int(value)
    if hasattr(value, "item"):
        v = value.item()
        return int(v) if isinstance(v, bool) else v
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
