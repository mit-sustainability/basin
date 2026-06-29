from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from dagster import Failure

from orchestrator.resources.arcgis import ArcGISResource, _serialize


@pytest.fixture
def resource():
    return ArcGISResource(
        client_id="test_client_id",
        client_secret="test_client_secret",
        org_url="https://www.arcgis.com",
    )


class TestGetToken:
    def test_returns_access_token(self, resource):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "tok123"}
        with patch("orchestrator.resources.arcgis.requests.post", return_value=mock_resp):
            token = resource._get_token()
        assert token == "tok123"

    def test_raises_on_error_body(self, resource):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"error": {"code": 400, "message": "Invalid credentials"}}
        with patch("orchestrator.resources.arcgis.requests.post", return_value=mock_resp):
            with pytest.raises(Failure, match="ArcGIS token error"):
                resource._get_token()

    def test_sends_client_credentials_grant(self, resource):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "tok"}
        with patch("orchestrator.resources.arcgis.requests.post", return_value=mock_resp) as mock_post:
            resource._get_token()
        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["data"]["grant_type"] == "client_credentials"
        assert call_kwargs.kwargs["data"]["client_id"] == "test_client_id"


class TestUpsertFeatures:
    def _make_existing(self, rows: list[dict]) -> list[dict]:
        return [{"OBJECTID": i + 1, **row} for i, row in enumerate(rows)]

    def test_splits_adds_and_updates(self, resource):
        existing = self._make_existing([
            {"sensor_id": "A", "datetime_edt": "2025-01-01T00:00:00"},
            {"sensor_id": "B", "datetime_edt": "2025-01-01T00:00:00"},
        ])
        incoming = pd.DataFrame([
            {"sensor_id": "B", "datetime_edt": "2025-01-01T00:00:00", "temperature_f": 80.0},
            {"sensor_id": "C", "datetime_edt": "2025-01-01T00:00:00", "temperature_f": 85.0},
        ])

        captured: dict = {}
        call_count = 0

        def mock_post(url, data=None, **kwargs):
            import json
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            if call_count == 1:
                resp.json.return_value = {"access_token": "tok"}
            else:
                captured["adds"] = json.loads(data["adds"])
                captured["updates"] = json.loads(data["updates"])
                resp.json.return_value = {
                    "addResults": [{"success": True}],
                    "updateResults": [{"success": True}],
                }
            return resp

        query_resp = MagicMock()
        query_resp.json.return_value = {
            "features": [{"attributes": e} for e in existing]
        }

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
            with patch("orchestrator.resources.arcgis.requests.get", return_value=query_resp):
                resource.upsert_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=incoming,
                    key_fields=["sensor_id", "datetime_edt"],
                )

        assert len(captured["adds"]) == 1
        assert captured["adds"][0]["attributes"]["sensor_id"] == "C"
        assert len(captured["updates"]) == 1
        assert captured["updates"][0]["attributes"]["sensor_id"] == "B"
        assert captured["updates"][0]["attributes"]["OBJECTID"] == 2

    def test_returns_add_update_counts(self, resource):
        apply_resp = MagicMock()
        apply_resp.json.return_value = {
            "addResults": [{"success": True}, {"success": True}],
            "updateResults": [{"success": True}],
        }
        token_resp = MagicMock()
        token_resp.json.return_value = {"access_token": "tok"}
        query_resp = MagicMock()
        query_resp.json.return_value = {"features": []}

        incoming = pd.DataFrame([
            {"sensor_id": "A", "datetime_edt": "2025-01-01", "temperature_f": 80.0},
            {"sensor_id": "B", "datetime_edt": "2025-01-01", "temperature_f": 85.0},
        ])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=[token_resp, apply_resp]):
            with patch("orchestrator.resources.arcgis.requests.get", return_value=query_resp):
                result = resource.upsert_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=incoming,
                    key_fields=["sensor_id", "datetime_edt"],
                )

        assert result["adds"] == 2
        assert result["updates"] == 1

    def test_raises_on_apply_edits_error(self, resource):
        token_resp = MagicMock()
        token_resp.json.return_value = {"access_token": "tok"}
        query_resp = MagicMock()
        query_resp.json.return_value = {"features": []}
        error_resp = MagicMock()
        error_resp.json.return_value = {"error": {"code": 500, "message": "Server error"}}

        incoming = pd.DataFrame([{"sensor_id": "A", "datetime_edt": "2025-01-01"}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=[token_resp, error_resp]):
            with patch("orchestrator.resources.arcgis.requests.get", return_value=query_resp):
                with pytest.raises(Failure, match="applyEdits error"):
                    resource.upsert_features(
                        layer_url="https://example.com/FeatureServer/0",
                        df=incoming,
                        key_fields=["sensor_id", "datetime_edt"],
                    )


class TestSerialize:
    def test_none_for_nan(self):
        import numpy as np
        assert _serialize(float("nan")) is None
        assert _serialize(np.nan) is None

    def test_numpy_scalar(self):
        import numpy as np
        val = np.float64(3.14)
        result = _serialize(val)
        assert isinstance(result, float)
        assert abs(result - 3.14) < 1e-6

    def test_timestamp_to_isoformat(self):
        ts = pd.Timestamp("2025-06-01 12:00:00")
        result = _serialize(ts)
        assert result == "2025-06-01T12:00:00"

    def test_passthrough_for_primitives(self):
        assert _serialize("hello") == "hello"
        assert _serialize(42) == 42


class TestReplaceFeatures:
    def _token_resp(self):
        r = MagicMock()
        r.json.return_value = {"access_token": "tok"}
        return r

    def _ids_resp(self, ids: list[int]):
        r = MagicMock()
        r.json.return_value = {"objectIds": ids}
        return r

    def _del_resp(self, n: int):
        r = MagicMock()
        r.json.return_value = {"deleteResults": [{"success": True}] * n}
        return r

    def _cap_resp(self):
        r = MagicMock()
        r.json.return_value = {"capabilities": "Create,Delete,Query,Update,Editing", "fields": []}
        return r

    def _routing(self, existing_ids: list[int], add_side_effect=None):
        """Build mock GET/POST handlers for the replace flow."""

        def mock_get(url, params=None, **kwargs):
            if "query" in url:
                return self._ids_resp(existing_ids)
            return self._cap_resp()

        post_calls = []

        def mock_post(url, data=None, **kwargs):
            post_calls.append((url, data))
            if url.endswith("/token"):
                return self._token_resp()
            if url.endswith("/addToDefinition"):
                r = MagicMock()
                r.json.return_value = {"success": True}
                return r
            if url.endswith("/deleteFeatures"):
                n = len((data or {}).get("objectIds", "").split(",")) if (data or {}).get("objectIds") else 0
                return self._del_resp(n)
            # addFeatures
            if add_side_effect:
                return add_side_effect(url, data)
            import json as _json
            batch_len = len(_json.loads(data["features"]))
            r = MagicMock()
            r.json.return_value = {"addResults": [{"success": True}] * batch_len}
            return r

        return mock_get, mock_post, post_calls

    def test_deletes_all_then_adds_all(self, resource):
        mock_get, mock_post, _ = self._routing(existing_ids=[1, 2])

        df = pd.DataFrame([
            {"sensor_id": "A", "temperature_f": 80.0},
            {"sensor_id": "B", "temperature_f": 85.0},
        ])

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                result = resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )

        assert result["deleted"] == 2
        assert result["added"] == 2

    def test_skips_delete_when_layer_empty(self, resource):
        mock_get, mock_post, post_calls = self._routing(existing_ids=[])

        df = pd.DataFrame([{"sensor_id": "A", "temperature_f": 80.0}])

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                result = resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )

        delete_calls = [url for url, _ in post_calls if "deleteFeatures" in url]
        assert delete_calls == []
        assert result["deleted"] == 0
        assert result["added"] == 1

    def test_attaches_point_geometry_when_geometry_fields_set(self, resource):
        import json as _json
        mock_get, mock_post, post_calls = self._routing(existing_ids=[])

        df = pd.DataFrame([{"sensor_id": "S1", "lat": 42.36, "lon": -71.09, "temperature_f": 80.0}])

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                    geometry_fields=("lat", "lon"),
                )

        add_data = next(data for url, data in post_calls if "addFeatures" in url)
        features = _json.loads(add_data["features"])
        assert features[0]["geometry"]["x"] == pytest.approx(-71.09)
        assert features[0]["geometry"]["y"] == pytest.approx(42.36)
        assert features[0]["geometry"]["spatialReference"]["wkid"] == 4326

    def test_no_geometry_when_geometry_fields_none(self, resource):
        import json as _json
        mock_get, mock_post, post_calls = self._routing(existing_ids=[])

        df = pd.DataFrame([{"sensor_id": "S1", "temperature_f": 80.0}])

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                    geometry_fields=None,
                )

        add_data = next(data for url, data in post_calls if "addFeatures" in url)
        features = _json.loads(add_data["features"])
        assert "geometry" not in features[0]

    def test_raises_on_ids_query_error(self, resource):
        err_resp = MagicMock()
        err_resp.json.return_value = {"error": {"code": 500, "message": "Server error"}}

        def mock_get(url, params=None, **kwargs):
            if "query" in url:
                return err_resp
            return self._cap_resp()

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", return_value=self._token_resp()):
                with pytest.raises(Failure, match="query \\(ids\\) error"):
                    resource.replace_features(
                        layer_url="https://example.com/FeatureServer/0",
                        df=pd.DataFrame([{"sensor_id": "A"}]),
                    )

    def test_raises_on_delete_error(self, resource):
        ids_resp = self._ids_resp([1])
        del_err = MagicMock()
        del_err.json.return_value = {"error": {"code": 500, "message": "Delete failed"}}

        def mock_get(url, params=None, **kwargs):
            if "query" in url:
                return ids_resp
            return self._cap_resp()

        def mock_post(url, data=None, **kwargs):
            if url.endswith("/token"):
                return self._token_resp()
            return del_err

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                with pytest.raises(Failure, match="deleteFeatures error"):
                    resource.replace_features(
                        layer_url="https://example.com/FeatureServer/0",
                        df=pd.DataFrame([{"sensor_id": "A"}]),
                    )

    def test_raises_on_add_error(self, resource):
        add_err = MagicMock()
        add_err.json.return_value = {"error": {"code": 500, "message": "Add failed"}}
        mock_get, mock_post, _ = self._routing(
            existing_ids=[],
            add_side_effect=lambda url, data: add_err,
        )

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                with pytest.raises(Failure, match="addFeatures error"):
                    resource.replace_features(
                        layer_url="https://example.com/FeatureServer/0",
                        df=pd.DataFrame([{"sensor_id": "A"}]),
                    )

    def test_raises_on_partial_add_failure(self, resource):
        import json as _json

        def add_partial(url, data):
            r = MagicMock()
            r.json.return_value = {
                "addResults": [{"success": True}, {"success": False, "error": {"code": 400}}]
            }
            return r

        mock_get, mock_post, _ = self._routing(
            existing_ids=[],
            add_side_effect=add_partial,
        )

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                with pytest.raises(Failure, match="record\\(s\\) failed"):
                    resource.replace_features(
                        layer_url="https://example.com/FeatureServer/0",
                        df=pd.DataFrame([{"sensor_id": "A"}, {"sensor_id": "B"}]),
                    )

    def test_batches_large_payloads(self, resource):
        import json as _json
        mock_get, mock_post, post_calls = self._routing(existing_ids=[])

        df = pd.DataFrame([{"sensor_id": f"S{i}"} for i in range(2500)])

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                result = resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )

        add_calls = [(url, data) for url, data in post_calls if "addFeatures" in url]
        assert len(add_calls) == 3  # ceil(2500/1000) = 3 batches
        batch_sizes = [len(_json.loads(data["features"])) for _, data in add_calls]
        assert batch_sizes == [1000, 1000, 500]
        assert result["added"] == 2500

    def test_batches_large_delete(self, resource):
        # 1100 existing features should result in 3 delete batches (500+500+100)
        existing = list(range(1, 1101))
        mock_get, mock_post, post_calls = self._routing(existing_ids=existing)

        df = pd.DataFrame([{"sensor_id": "A"}])

        with patch("orchestrator.resources.arcgis.requests.get", side_effect=mock_get):
            with patch("orchestrator.resources.arcgis.requests.post", side_effect=mock_post):
                result = resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )

        delete_calls = [(url, data) for url, data in post_calls if "deleteFeatures" in url]
        assert len(delete_calls) == 3
        assert result["deleted"] == 1100
