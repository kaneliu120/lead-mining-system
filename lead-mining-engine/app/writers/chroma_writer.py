"""
ChromaWriter — vector database write layer
Uses ChromaDB for semantic search (RAG queries), designed for the sales-outreach engine
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.models.lead import EnrichedLead, LeadRaw

logger = logging.getLogger(__name__)


class ChromaWriter:
    """
    ChromaDB HTTP Client wrapper (connects to external ChromaDB service in DockerCompose).
    Collections are partitioned by industry_keyword.
    """

    DEFAULT_COLLECTION = "all_leads"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8001,
        collection_name: str = DEFAULT_COLLECTION,
        embedding_model: str = "all-MiniLM-L6-v2",
        auth_token: str = "",
        use_ssl: bool = False,
    ):
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        self.auth_token = auth_token
        self.use_ssl = use_ssl
        self._client = None
        self._collection = None

    def connect(self) -> None:
        """Initialize ChromaDB client (synchronous), supports auth and SSL"""
        try:
            import chromadb
            from chromadb.config import Settings
        except ImportError:
            raise ImportError("chromadb not installed. Run: pip install chromadb")

        try:
            from chromadb.utils import embedding_functions
            ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name=self.embedding_model
            )
            logger.info("ChromaWriter using SentenceTransformer: %s", self.embedding_model)
        except Exception as exc:
            logger.warning(
                "SentenceTransformerEmbeddingFunction unavailable (%s), "
                "falling back to ChromaDB default embedding. "
                "Vectors may be inconsistent if collection was created with a different EF.",
                exc,
            )
            ef = None

        # Build connection parameters
        client_kwargs = {
            "host": self.host,
            "port": self.port,
        }

        # Configure auth
        settings_kwargs = {}
        if self.auth_token:
            settings_kwargs["chroma_client_auth_provider"] = (
                "chromadb.auth.token_authn.TokenAuthClientProvider"
            )
            settings_kwargs["chroma_client_auth_credentials"] = self.auth_token
            logger.info("ChromaWriter: token authentication enabled")

        # Configure SSL
        if self.use_ssl:
            client_kwargs["ssl"] = True
            logger.info("ChromaWriter: SSL/TLS enabled")

        if settings_kwargs:
            client_kwargs["settings"] = Settings(**settings_kwargs)

        self._client = chromadb.HttpClient(**client_kwargs)
        self._collection = self._client.get_or_create_collection(
            name=self.collection_name,
            embedding_function=ef,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info(
            f"ChromaWriter connected to {self.host}:{self.port}, "
            f"collection='{self.collection_name}'"
        )

    def upsert_leads(self, leads: List[LeadRaw]) -> int:
        """
        Convert lead to a ChromaDB Document and upsert.
        The document field is used for embedding; metadata stores structured fields for filtering.
        """
        if self._collection is None:
            raise RuntimeError("ChromaWriter not connected")

        docs, ids, metas = [], [], []
        for lead in leads:
            doc_text = lead.to_chroma_document()       # Returns a plain-text string
            ids.append(lead.dedup_key())               # Use dedup_key as ChromaDB document ID
            docs.append(doc_text)
            metas.append({
                "source":           lead.source.value,
                "business_name":    lead.business_name,
                "industry_keyword": lead.industry_keyword,
                "address":          lead.address or "",
                "phone":            lead.phone or "",
                "website":          lead.website or "",
                "score":            lead.score or 0,
            })

        if not docs:
            return 0

        # Chroma upsert supports batch (insert if not exists, update if exists)
        self._collection.upsert(
            ids=ids,
            documents=docs,
            metadatas=metas,
        )
        logger.info(f"ChromaWriter upserted {len(docs)} documents")
        return len(docs)

    def query_similar(
        self,
        query_text: str,
        n_results: int = 10,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Semantic similarity query for use by the sales-outreach RAG.
        Return format: [{id, document, distance, metadata}, ...]
        """
        if self._collection is None:
            raise RuntimeError("ChromaWriter not connected")

        # ChromaDB raises ValueError when n_results > collection size
        count = self._collection.count()
        if count == 0:
            logger.debug("ChromaWriter: collection is empty, returning no results")
            return []
        n_results = min(n_results, count)

        kwargs: Dict[str, Any] = {
            "query_texts": [query_text],
            "n_results": n_results,
        }
        if where:
            kwargs["where"] = where

        try:
            results = self._collection.query(**kwargs)
        except Exception as exc:
            logger.error("ChromaWriter query failed: %s", exc)
            return []

        output = []
        for i, doc_id in enumerate(results["ids"][0]):
            output.append({
                "id":       doc_id,
                "document": results["documents"][0][i],
                "distance": results["distances"][0][i] if results.get("distances") else None,
                "metadata": results["metadatas"][0][i] if results.get("metadatas") else {},
            })
        return output

    def delete_by_source(self, source: str) -> int:
        """Delete all documents for a given source (to clear old data before re-mining)"""
        if self._collection is None:
            raise RuntimeError("ChromaWriter not connected")

        result = self._collection.delete(where={"source": source})
        count = len(result) if isinstance(result, list) else 0
        logger.info(f"ChromaWriter deleted {count} docs for source={source}")
        return count

    def count(self) -> int:
        """Return total document count in the collection"""
        if self._collection is None:
            return 0
        return self._collection.count()
