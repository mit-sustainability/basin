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
    def _route(self, token_resp, delete_resp, add_resp):
        """Return a side_effect function that routes POST calls by URL."""
        def mock_post(url, data=None, **kwargs):
            if url.endswith("/token"):
                return token_resp
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            else:
                return add_resp
        return mock_post

    def _token_resp(self):
        r = MagicMock()
        r.json.return_value = {"access_token": "tok"}
        return r

    def test_deletes_all_then_adds_all(self, resource):
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": [{"success": True}, {"success": True}]}
        add_resp = MagicMock()
        add_resp.json.return_value = {"addResults": [{"success": True}, {"success": True}]}

        df = pd.DataFrame([
            {"sensor_id": "A", "temperature_f": 80.0},
            {"sensor_id": "B", "temperature_f": 85.0},
        ])

        with patch("orchestrator.resources.arcgis.requests.post",
                   side_effect=self._route(self._token_resp(), delete_resp, add_resp)):
            result = resource.replace_features(
                layer_url="https://example.com/FeatureServer/0",
                df=df,
            )

        assert result["deleted"] == 2
        assert result["added"] == 2

    def test_attaches_point_geometry_when_geometry_fields_set(self, resource):
        import json as _json
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": []}
        add_resp = MagicMock()
        add_resp.json.return_value = {"addResults": [{"success": True}]}

        post_calls = []

        def routing_post(url, data=None, **kwargs):
            post_calls.append((url, data))
            if url.endswith("/token"):
                return self._token_resp()
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            return add_resp

        df = pd.DataFrame([{"sensor_id": "S1", "lat": 42.36, "lon": -71.09, "temperature_f": 80.0}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
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
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": []}
        add_resp = MagicMock()
        add_resp.json.return_value = {"addResults": [{"success": True}]}

        post_calls = []

        def routing_post(url, data=None, **kwargs):
            post_calls.append((url, data))
            if url.endswith("/token"):
                return self._token_resp()
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            return add_resp

        df = pd.DataFrame([{"sensor_id": "S1", "temperature_f": 80.0}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            resource.replace_features(
                layer_url="https://example.com/FeatureServer/0",
                df=df,
                geometry_fields=None,
            )

        add_data = next(data for url, data in post_calls if "addFeatures" in url)
        features = _json.loads(add_data["features"])
        assert "geometry" not in features[0]

    def test_raises_on_delete_error(self, resource):
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"error": {"code": 500, "message": "Server error"}}

        def routing_post(url, data=None, **kwargs):
            if url.endswith("/token"):
                return self._token_resp()
            return delete_resp

        df = pd.DataFrame([{"sensor_id": "A"}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            with pytest.raises(Failure, match="deleteFeatures error"):
                resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )

    def test_raises_on_add_error(self, resource):
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": []}
        add_resp = MagicMock()
        add_resp.json.return_value = {"error": {"code": 500, "message": "Add failed"}}

        def routing_post(url, data=None, **kwargs):
            if url.endswith("/token"):
                return self._token_resp()
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            return add_resp

        df = pd.DataFrame([{"sensor_id": "A"}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            with pytest.raises(Failure, match="addFeatures error"):
                resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )

    def test_raises_on_partial_add_failure(self, resource):
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": []}
        add_resp = MagicMock()
        add_resp.json.return_value = {
            "addResults": [{"success": True}, {"success": False, "error": {"code": 400}}]
        }

        def routing_post(url, data=None, **kwargs):
            if url.endswith("/token"):
                return self._token_resp()
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            return add_resp

        df = pd.DataFrame([{"sensor_id": "A"}, {"sensor_id": "B"}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            with pytest.raises(Failure, match="record\\(s\\) failed"):
                resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )

    def test_batches_large_payloads(self, resource):
        import json as _json
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": []}

        post_calls = []

        def routing_post(url, data=None, **kwargs):
            post_calls.append((url, data))
            if url.endswith("/token"):
                return self._token_resp()
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            # Return success count matching actual batch size
            batch_len = len(_json.loads(data["features"]))
            r = MagicMock()
            r.json.return_value = {"addResults": [{"success": True}] * batch_len}
            return r

        df = pd.DataFrame([{"sensor_id": f"S{i}"} for i in range(2500)])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            result = resource.replace_features(
                layer_url="https://example.com/FeatureServer/0",
                df=df,
            )

        add_calls = [(url, data) for url, data in post_calls if "addFeatures" in url]
        assert len(add_calls) == 3  # ceil(2500/1000) = 3 batches
        batch_sizes = [len(_json.loads(data["features"])) for _, data in add_calls]
        assert batch_sizes == [1000, 1000, 500]
        assert result["added"] == 2500
