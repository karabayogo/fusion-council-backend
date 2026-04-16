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
