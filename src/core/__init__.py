"""Core Module - Configuración y componentes centrales."""

from .auth import get_current_user, verify_credentials, parse_basic_auth

__all__ = [
    "get_current_user",
    "verify_credentials",
    "parse_basic_auth",
]
