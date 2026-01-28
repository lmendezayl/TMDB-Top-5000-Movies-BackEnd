#!/usr/bin/env python
"""Script de entrada para ejecutar la aplicación FastAPI."""

import sys
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.absolute()
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

if __name__ == "__main__":
    """Inicia la aplicación FastAPI con Uvicorn."""
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "main:app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
        "--reload",
        "--log-level",
        "info",
    ]
    
    print(f"Iniciando Data Lake API...")
    print(f"Directorio de trabajo: {PROJECT_ROOT}")
    print(f"SRC path: {SRC_PATH}")
    print(f"\nComando: {' '.join(cmd)}\n")
    
    import os
    os.chdir(SRC_PATH)
    
    subprocess.run(cmd)
