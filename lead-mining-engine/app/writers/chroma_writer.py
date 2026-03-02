"""
ChromaWriter — 向量数据库写入层
使用 ChromaDB 支持语义搜索（RAG 查询），适合 sales-outreach 引擎调用
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.models.lead import EnrichedLead, LeadRaw

logger = logging.getLogger(__name__)


class ChromaWriter:
    """
    ChromaDB HTTP Client 封装（连接外部 DockerCompose 中的 ChromaDB 服务）。
    Collection 按 industry_keyword 分区。
    """

    DEFAULT_COLLECTION = "all_leads"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8001,
        collection_name: str = DEFAULT_COLLECTION,
        embedding_model: str = "all-MiniLM-L6-v2",
    ):
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        self._client = None
        self._collection = None

    def connect(self) -> None:
        """初始化 ChromaDB 客户端（同步）"""
        try:
            import chromadb
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

        self._client = chromadb.HttpClient(host=self.host, port=self.port)
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
        将线索转换为 ChromaDB Document 并 upsert。
        document 字段用于嵌入，metadata 保存结构化字段供过滤。
        """
        if self._collection is None:
            raise RuntimeError("ChromaWriter not connected")

        docs, ids, metas = [], [], []
        for lead in leads:
            doc_text = lead.to_chroma_document()       # 返回纯文本字符串
            ids.append(lead.dedup_key())               # 用 dedup_key 作为 ChromaDB 文档 ID
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

        # Chroma upsert 支持批量（不存在则 insert，存在则 update）
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
        语义相似度查询，供 sales-outreach RAG 使用。
        返回格式：[{id, document, distance, metadata}, ...]
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
        """删除某个 source 的所有文档（用于重新采集时清空旧数据）"""
        if self._collection is None:
            raise RuntimeError("ChromaWriter not connected")

        result = self._collection.delete(where={"source": source})
        count = len(result) if isinstance(result, list) else 0
        logger.info(f"ChromaWriter deleted {count} docs for source={source}")
        return count

    def count(self) -> int:
        """返回 collection 中的文档总数"""
        if self._collection is None:
            return 0
        return self._collection.count()
