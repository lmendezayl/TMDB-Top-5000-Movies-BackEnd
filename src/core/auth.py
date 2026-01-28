"""Authentication Module - Autenticación Basic Auth para FastAPI."""

import base64
from typing import Optional, Tuple
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

# Credenciales de ejemplo (en producción, usar BD o variables de entorno)
VALID_CREDENTIALS: dict[str, str] = {
    "admin": "password123",
    "user": "user123",
}

security = HTTPBasic()


def get_current_user(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """
    Valida las credenciales Basic Auth y retorna el username.
    
    Raises:
        HTTPException: Si las credenciales son inválidas.
    """
    username = credentials.username
    password = credentials.password
    
    # Verificar si el usuario existe y la contraseña es correcta
    if username not in VALID_CREDENTIALS or VALID_CREDENTIALS[username] != password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas",
            headers={"WWW-Authenticate": "Basic"},
        )
    
    return username


def verify_credentials(username: str, password: str) -> bool:
    """Verifica si las credenciales son válidas."""
    return username in VALID_CREDENTIALS and VALID_CREDENTIALS[username] == password


def parse_basic_auth(authorization: str) -> Tuple[str, str]:
    """
    Parsea la cabecera de Autorización Basic Auth.
    
    Format: Authorization: Basic <base64(username:password)>
    """
    try:
        if not authorization.startswith("Basic "):
            raise ValueError("Invalid authorization header")
        
        credentials = base64.b64decode(authorization[6:]).decode("utf-8")
        username, password = credentials.split(":", 1)
        return username, password
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Formato de autenticación inválido",
            headers={"WWW-Authenticate": "Basic"},
        )
