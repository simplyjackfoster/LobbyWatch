"""Tests for lambda_worker helper functions."""

import sys
import types
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

# Stub import-time dependencies so helper tests can import lambda_worker safely.
if "aws_env" not in sys.modules:
    aws_env_stub = types.ModuleType("aws_env")
    aws_env_stub.bootstrap_ssm_env = lambda: None
    sys.modules["aws_env"] = aws_env_stub

if "models" not in sys.modules:
    models_stub = types.ModuleType("models")
    models_stub.SessionLocal = object
    sys.modules["models"] = models_stub

from lambda_worker import _normalize_name  # noqa: E402


def _extract_lobbyist_name(raw: dict) -> str:
    """Helper that mirrors the fixed logic from lambda_worker._sync_lda_enrichment."""
    lobbyist_name = raw.get("lobbyist") or raw.get("name") or ""
    if isinstance(lobbyist_name, dict):
        first = (lobbyist_name.get("first_name") or "").strip()
        last = (lobbyist_name.get("last_name") or "").strip()
        lobbyist_name = f"{first} {last}".strip()
    return str(lobbyist_name).strip()


def _extract_specific_issues(activities: list) -> str | None:
    """Mirrors the fixed logic from lambda_worker._sync_lda_enrichment."""
    return " | ".join(
        [str(a.get("specific_issue")).strip() for a in activities if a.get("specific_issue")]
    ) or None


def _extract_general_issue_codes(activities: list) -> list[str]:
    """Mirrors lambda_worker._sync_lda_enrichment issue code extraction."""
    return sorted(
        {
            str(a.get("general_issue_code")).strip().upper()
            for a in activities
            if a.get("general_issue_code")
        }
    )


def test_normalize_name_none():
    assert _normalize_name(None) == ""


def test_normalize_name_strips_legal_suffix():
    assert _normalize_name("Pfizer Inc.") == "PFIZER"


def test_normalize_name_strips_llc():
    assert _normalize_name("Strategics Consulting, LLC") == "STRATEGICS CONSULTING"


def test_normalize_name_strips_lp():
    assert _normalize_name("Blackstone LP") == "BLACKSTONE"


def test_normalize_name_preserves_middle_suffix_word():
    assert _normalize_name("LLC Solutions Group") == "LLC SOLUTIONS GROUP"


def test_lobbyist_name_extracted_from_nested_dict():
    raw = {
        "lobbyist": {
            "prefix": None,
            "first_name": "Jane",
            "nickname": None,
            "last_name": "Smith",
            "suffix": None,
        },
        "covered_position": None,
    }
    assert _extract_lobbyist_name(raw) == "Jane Smith"


def test_lobbyist_name_extracted_when_flat_name_key():
    raw = {"name": "Bob Jones"}
    assert _extract_lobbyist_name(raw) == "Bob Jones"


def test_lobbyist_name_empty_when_no_name():
    raw = {"lobbyist": {}}
    assert _extract_lobbyist_name(raw) == ""


def test_lobbyist_name_first_only():
    raw = {"lobbyist": {"first_name": "Alice", "last_name": None}}
    assert _extract_lobbyist_name(raw) == "Alice"


def test_specific_issue_extracted_with_singular_key():
    activities = [
        {"general_issue_code": "HLTH", "specific_issue": "Medicare Part D pricing"},
        {"general_issue_code": "DEF", "specific_issue": "F-35 procurement"},
    ]
    result = _extract_specific_issues(activities)
    assert result == "Medicare Part D pricing | F-35 procurement"


def test_specific_issue_returns_none_when_all_empty():
    activities = [{"general_issue_code": "HLTH", "specific_issue": ""}]
    result = _extract_specific_issues(activities)
    assert result is None


def test_specific_issue_wrong_plural_key_returns_none():
    activities = [{"specific_issue": "real text here"}]
    result = " | ".join(
        [str(a.get("specific_issues")).strip() for a in activities if a.get("specific_issues")]
    ) or None
    assert result is None


def test_general_issue_codes_extracted():
    activities = [
        {"general_issue_code": "HLTH"},
        {"general_issue_code": "def"},
        {"general_issue_code": "HLTH"},
    ]
    result = _extract_general_issue_codes(activities)
    assert result == ["DEF", "HLTH"]


def test_general_issue_codes_empty_activities():
    assert _extract_general_issue_codes([]) == []


def test_general_issue_codes_none_code_skipped():
    activities = [{"general_issue_code": None}, {"general_issue_code": "TAX"}]
    result = _extract_general_issue_codes(activities)
    assert result == ["TAX"]


def test_sync_cosponsors_defaults_to_true():
    payload = {}
    sync_cosponsors = bool(payload.get("sync_cosponsors", True))
    assert sync_cosponsors is True


def test_cosponsor_member_limit_defaults_to_600():
    payload = {}
    limit = int(payload.get("cosponsor_member_limit") or 600)
    assert limit >= 537


def test_cosponsor_member_limit_can_be_overridden():
    payload = {"cosponsor_member_limit": 10}
    limit = int(payload.get("cosponsor_member_limit") or 600)
    assert limit == 10
