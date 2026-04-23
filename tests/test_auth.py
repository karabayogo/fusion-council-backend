"""Tests for auth module."""

from fusion_council_service.auth import (
    constant_time_compare,
    extract_bearer,
    hash_token,
    resolve_role,
)


def test_extract_bearer_valid():
    assert extract_bearer("Bearer my-token") == "my-token"


def test_extract_bearer_case_insensitive():
    assert extract_bearer("bearer MY-TOKEN") == "MY-TOKEN"


def test_extract_bearer_missing():
    assert extract_bearer(None) is None
    assert extract_bearer("") is None
    assert extract_bearer("Basic abc") is None


def test_hash_token_deterministic():
    h1 = hash_token("secret-key")
    h2 = hash_token("secret-key")
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_constant_time_compare():
    assert constant_time_compare("abc", "abc") is True
    assert constant_time_compare("abc", "abd") is False
    assert constant_time_compare("abc", "ab") is False


def test_resolve_role_admin():
    assert resolve_role("admin-key", [], ["admin-key"]) == "admin"


def test_resolve_role_user():
    assert resolve_role("user-key", ["user-key"], ["admin-key"]) == "user"


def test_resolve_role_unknown():
    assert resolve_role("bad-key", ["user-key"], ["admin-key"]) is None





def test_auth_dependency_accepts_header(tmp_db, mock_settings):
    """Standard Authorization: Bearer header should be accepted."""
    from fusion_council_service.api.routes import init_api, get_auth_dependency
    init_api(mock_settings)

    dep = get_auth_dependency()
    result = dep(authorization="Bearer test-user-key")
    assert result == ("test-user-key", "user")


def test_auth_dependency_accepts_query_param(tmp_db, mock_settings):
    """Query param ?auth=<token> should be accepted as fallback."""
    from fusion_council_service.api.routes import init_api, get_auth_dependency
    init_api(mock_settings)

    dep = get_auth_dependency()
    result = dep(authorization=None, auth_query="test-user-key")
    assert result == ("test-user-key", "user")


def test_auth_dependency_header_takes_precedence(tmp_db, mock_settings):
    """Header token takes precedence over query param when both are provided."""
    from fusion_council_service.api.routes import init_api, get_auth_dependency
    init_api(mock_settings)

    dep = get_auth_dependency()
    result = dep(authorization="Bearer test-admin-key", auth_query="test-user-key")
    assert result == ("test-admin-key", "admin")


def test_auth_dependency_rejects_missing_token(tmp_db, mock_settings):
    """Missing both header and query param should raise 401."""
    from fusion_council_service.api.routes import init_api, get_auth_dependency
    from fastapi import HTTPException
    init_api(mock_settings)

    dep = get_auth_dependency()
    try:
        dep(authorization=None, auth_query=None)
        assert False, "Expected HTTPException"
    except HTTPException as e:
        assert e.status_code == 401
        assert "Missing Authorization header" in e.detail


def test_auth_dependency_rejects_invalid_token(tmp_db, mock_settings):
    """Invalid token not in any key list should raise 401."""
    from fusion_council_service.api.routes import init_api, get_auth_dependency
    from fastapi import HTTPException
    init_api(mock_settings)

    dep = get_auth_dependency()
    try:
        dep(authorization="Bearer invalid-token", auth_query=None)
        assert False, "Expected HTTPException"
    except HTTPException as e:
        assert e.status_code == 401
        assert "Invalid API key" in e.detail
