#!/usr/bin/env python
"""Script para limpiar/resetear la base de datos y archivos generados."""

import sys
import shutil
from pathlib import Path

def reset_database():
    """Elimina archivos generados durante ETL y BD."""
    
    print("⚠️  RESET DATABASE")
    print("="*70)
    
    files_to_remove = [
        "data/gold/warehouse.db",
        "data/gold/warehouse.db-shm",
        "data/gold/warehouse.db-wal",
    ]
    
    folders_to_clean = [
        "data/silver",
        "data/gold",
    ]
    
    print("\n🗑️  Eliminando archivos...")
    for file_path in files_to_remove:
        p = Path(file_path)
        if p.exists():
            p.unlink()
            print(f"  ✓ Eliminado: {file_path}")
    
    print("\n📁 Limpiando carpetas...")
    for folder_path in folders_to_clean:
        p = Path(folder_path)
        if p.exists():
            for file in p.glob("*"):
                if file.is_file():
                    file.unlink()
                    print(f"  ✓ Eliminado: {file}")
                elif file.is_dir():
                    shutil.rmtree(file)
                    print(f"  ✓ Eliminada carpeta: {file}")
    
    print("\n✏️  Recreando estructura...")
    for folder_path in folders_to_clean:
        p = Path(folder_path)
        p.mkdir(parents=True, exist_ok=True)
        print(f"  ✓ Verificada carpeta: {folder_path}")
    
    print("\n" + "="*70)
    print("✅ BASE DE DATOS Y ARCHIVOS RESETEADOS")
    print("="*70)

    print("\n🧹 Intentando resetear Typesense...")
    try:
        project_root = Path(__file__).parent.absolute()
        src_path = project_root / "src"
        if str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))
        
        from infrastructure.typesense_client import TypesenseClient
        from infrastructure.typesense_indexer import TypesenseIndexer
        
        client = TypesenseClient(host="localhost", port=8108)
        indexer = TypesenseIndexer(client=client)
        
        if client.health_check():
            if indexer.recreate_collection():
                print("  ✓ Colección Typesense 'movies' recreada y vaciada")
            else:
                print("  ⚠ No se pudo recrear la colección (error interno)")
        else:
             print("  ⚠ Typesense no disponible en localhost:8108 (¿Docker está corriendo?)")
             print("    Si Typesense no está accesible, la colección vieja persistirá.")
             
    except ImportError:
        print("  ⚠ Librería 'typesense' no instalada en el entorno local. Saltando.")
    except Exception as e:
        print(f"  ⚠ Error reseteando Typesense: {e}")

    print("\nPróximos pasos:")
    print("  1. Ejecuta: python run.py (o docker-compose up --build)")
    print("  2. El ETL volverá a generar los datos")
    print("  3. Las tablas se crearán nuevamente en SQLite")
    print("  4. Typesense se re-indexará automáticamente al inicio")

if __name__ == "__main__":
    try:
        reset_database()
    except Exception as e:
        print(f"\n❌ Error durante reset: {e}", file=sys.stderr)
        sys.exit(1)
