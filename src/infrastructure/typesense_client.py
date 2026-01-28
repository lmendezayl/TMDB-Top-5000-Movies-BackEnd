"""Typesense Client - Vector Database Integration."""

import logging
from typing import Optional, Dict, Any, List
import os

try:
    import typesense
except ImportError:
    typesense = None

logger = logging.getLogger(__name__)


class TypesenseClient:
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        api_key: Optional[str] = None,
    ):
        if typesense is None:
            self.enabled = False
            logger.warning("typesense package not installed")
            return

        # Usar variable de entorno o default
        import os
        host = host or os.getenv("TYPESENSE_HOST", "typesense")
        port = port or int(os.getenv("TYPESENSE_PORT", "8108"))
        api_key = api_key or os.getenv("TYPESENSE_API_KEY", "ts-xyz123456")
        
        self.host = host
        self.port = port
        
        self.client = typesense.Client({
            "nodes": [{"host": host, "port": port, "protocol": "http"}],
            "api_key": api_key,
            "connection_timeout_seconds": 2,
        })
        logger.info(f"Typesense client initialized: {self.host}:{self.port}")

    def health_check(self) -> bool:
        """Verifica si Typesense está disponible."""
        try:
            # Opción más segura: Intentar hacer un health check explícito
            return self.client.operations.is_healthy()
        except Exception:
            try:
                # Fallback: Intentar listar colecciones
                self.client.collections.retrieve()
                return True
            except Exception as e:
                logger.error(f"Typesense health check failed: {e}")
                return False

    def create_collection(
        self, name: str, schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Crea una colección en Typesense.

        Args:
            name: Nombre de la colección
            schema: Schema de la colección (fields, default_sorting_field, etc.)

        Returns:
            Respuesta del servidor
        """
        try:
            response = self.client.collections.create(schema)
            logger.info(f"Collection '{name}' created successfully")
            return response
        except Exception as e:
            logger.error(f"Error creating collection '{name}': {e}")
            raise

    def collection_exists(self, name: str) -> bool:
        """
        Verifica si una colección existe.

        Args:
            name: Nombre de la colección

        Returns:
            True si la colección existe
        """
        try:
            self.client.collections[name].retrieve()
            return True
        except Exception:
            return False

    def delete_collection(self, name: str) -> bool:
        """
        Elimina una colección.

        Args:
            name: Nombre de la colección

        Returns:
            True si se eliminó exitosamente
        """
        try:
            self.client.collections[name].delete()
            logger.info(f"Collection '{name}' deleted")
            return True
        except Exception as e:
            logger.error(f"Error deleting collection '{name}': {e}")
            return False

    def upsert_documents(self, collection_name: str, documents: List[Dict[str, Any]]) -> bool:
        """
        Inserta o actualiza múltiples documentos.
        """
        try:
            self.client.collections[collection_name].documents.import_(
                documents, {"action": "upsert"}
            )
            return True
        except Exception as e:
            logger.error(f"Error upserting documents: {e}")
            
            # DEBUG: Imprimir el primer documento para ver qué tiene mal
            if documents:
                import json
                try:
                    logger.error(f"First failing doc sample: {json.dumps(documents[0], default=str)}")
                except:
                    logger.error(f"Could not dump doc: {documents[0]}")
            
            return False

    def search(
        self,
        collection: str,
        query_text: str,
        search_field: str = "title",
        limit: int = 10,
        offset: int = 0,
        filter_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Busca documentos en una colección.

        Args:
            collection: Nombre de la colección
            query_text: Texto a buscar
            search_field: Campo en el que buscar
            limit: Número máximo de resultados
            offset: Desplazamiento para paginación
            filter_by: Filtro adicional (ej: "popularity:[8,10]")

        Returns:
            Resultados de búsqueda
        """
        try:
            search_params = {
                "q": query_text,
                "query_by": search_field,
                "limit": limit,
                "offset": offset,
            }
            if filter_by:
                search_params["filter_by"] = filter_by

            results = self.client.collections[collection].documents.search(search_params)
            logger.info(
                f"Search in '{collection}': {len(results.get('hits', []))} results"
            )
            return results
        except Exception as e:
            logger.error(f"Error searching: {e}")
            raise

    def get_document(self, collection: str, doc_id: str) -> Dict[str, Any]:
        """
        Obtiene un documento por ID.

        Args:
            collection: Nombre de la colección
            doc_id: ID del documento

        Returns:
            Documento
        """
        try:
            doc = self.client.collections[collection].documents[doc_id].retrieve()
            return doc
        except Exception as e:
            logger.error(f"Error getting document {doc_id}: {e}")
            raise

    def delete_document(self, collection: str, doc_id: str) -> bool:
        """
        Elimina un documento.

        Args:
            collection: Nombre de la colección
            doc_id: ID del documento

        Returns:
            True si se eliminó exitosamente
        """
        try:
            self.client.collections[collection].documents[doc_id].delete()
            logger.info(f"Document {doc_id} deleted from '{collection}'")
            return True
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
            raise

    def count_documents(self, collection: str) -> int:
        """
        Cuenta documentos en una colección.

        Args:
            collection: Nombre de la colección

        Returns:
            Número de documentos
        """
        try:
            result = self.client.collections[collection].retrieve()
            return result.get("num_documents", 0)
        except Exception as e:
            logger.error(f"Error counting documents: {e}")
            return 0

    def batch_search(
        self,
        collection: str,
        queries: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Ejecuta múltiples búsquedas en batch.

        Args:
            collection: Nombre de la colección
            queries: Lista de queries con parámetros

        Returns:
            Lista de resultados
        """
        try:
            results = self.client.collections[collection].documents.search(
                {"searches": queries}
            )
            return results
        except Exception as e:
            logger.error(f"Error in batch search: {e}")
            raise
