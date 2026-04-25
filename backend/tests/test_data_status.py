import sys
from pathlib import Path

from fastapi.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from main import app, get_db


class _MissingTableDb:
    def execute(self, *_args, **_kwargs):
        raise RuntimeError("relation _pipeline_meta does not exist")

    def rollback(self):
        return None



def test_meta_data_status_returns_nulls_when_pipeline_meta_unavailable():
    def _override_db():
        return _MissingTableDb()

    app.dependency_overrides[get_db] = _override_db
    try:
        client = TestClient(app)
        response = client.get("/meta/data-status")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "last_exported_at": None,
        "last_ingest_at": None,
        "lda_coverage_through": None,
        "congress_coverage_through": None,
    }
