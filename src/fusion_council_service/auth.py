"""Bearer token authentication and role resolution."""

import hashlib
import hmac
from typing import Optional

from fastapi import Header, HTTPException


def extract_bearer(authorization: Optional[str]) -> Optional[str]:
    """Extract the token from an Authorization: Bearer <token> header."""
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip()


def constant_time_compare(a: str, b: str) -> bool:
    """Constant-time string comparison to prevent timing attacks."""
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def hash_token(token: str) -> str:
    """SHA-256 hash of a token for storage in owner_token_hash."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def resolve_role(token: str, api_keys: list[str], admin_keys: list[str]) -> Optional[str]:
    """Resolve a token to 'admin', 'user', or None."""
    for admin_key in admin_keys:
        if constant_time_compare(token, admin_key):
            return "admin"
    for api_key in api_keys:
        if constant_time_compare(token, api_key):
            return "user"
    return None


def require_auth(
    authorization: Optional[str] = Header(None),
) -> tuple[str, str]:
    """FastAPI dependency: returns (token, role) or raises 401/403."""

    # We need the settings — this is resolved at request time from app.state
    # This dependency is wrapped by a factory that injects settings
    token = extract_bearer(authorization)
    if token is None:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    # This will be replaced with a proper dependency injection approach
    # For now, we raise and the route handler does the lookup
    return token, "unknown"  # role resolved by caller with settings


def make_auth_dependency(settings):
    """Create a FastAPI dependency closure with settings bound."""
    def auth_dependency(
        authorization: Optional[str] = Header(None),
    ) -> tuple[str, str]:
        token = extract_bearer(authorization)
        if token is None:
            raise HTTPException(status_code=401, detail="Missing Authorization header")

        role = resolve_role(token, settings.service_api_keys, settings.service_admin_api_keys)
        if role is None:
            raise HTTPException(status_code=401, detail="Invalid API key")

        return token, role

    return auth_dependency


def require_admin(token: str, role: str) -> None:
    """Raise 403 if the token does not have admin role."""
    if role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")