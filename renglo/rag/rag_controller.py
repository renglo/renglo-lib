from __future__ import annotations

from typing import Any, Dict, List, Optional

import boto3

from renglo.logger import get_logger


class RagConfigError(Exception):
    pass


class RagController:
    """Thin wrapper around Bedrock Agent Runtime retrieve / retrieve_and_generate."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        bedrock_client: Optional[Any] = None,
        region_name: Optional[str] = None,
    ):
        self.config = config or {}
        self.logger = get_logger()
        self.kb_id = (self.config.get("KB_ID") or "").strip()
        self.model_arn = (self.config.get("RAG_MODEL_ARN") or "").strip()
        resolved_region = region_name or self.config.get("AWS_REGION", "us-east-1")
        self.client = bedrock_client or boto3.client(
            "bedrock-agent-runtime",
            region_name=resolved_region,
        )

    def _require_kb_id(self) -> str:
        if not self.kb_id:
            raise RagConfigError("KB_ID configuration is required")
        return self.kb_id

    def _require_model_arn(self) -> str:
        if not self.model_arn:
            raise RagConfigError("RAG_MODEL_ARN configuration is required for rag_generate")
        return self.model_arn

    def _normalize_retrieval_results(self, raw_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for item in raw_results or []:
            content = item.get("content") or {}
            location = item.get("location") or {}
            metadata = item.get("metadata") or {}
            results.append(
                {
                    "text": content.get("text", ""),
                    "score": item.get("score"),
                    "location": location,
                    "metadata": metadata,
                }
            )
        return results

    def _normalize_citations(self, citations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for citation in citations or []:
            generated = citation.get("generatedResponsePart") or {}
            text_part = generated.get("textResponsePart") or {}
            refs = []
            for ref in citation.get("retrievedReferences") or []:
                content = ref.get("content") or {}
                refs.append(
                    {
                        "text": content.get("text", ""),
                        "location": ref.get("location") or {},
                        "metadata": ref.get("metadata") or {},
                    }
                )
            normalized.append(
                {
                    "text": text_part.get("text", ""),
                    "span": text_part.get("span") or {},
                    "references": refs,
                }
            )
        return normalized

    def rag_retrieve(
        self,
        query: str,
        *,
        number_of_results: int = 5,
        next_token: Optional[str] = None,
        retrieval_configuration: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Retrieve relevant chunks from the configured Knowledge Base.

        Equivalent to bedrock-agent-runtime Retrieve.
        """
        kb_id = self._require_kb_id()
        if not isinstance(query, str) or not query.strip():
            return {
                "success": False,
                "action": "rag_retrieve",
                "error": "query must be a non-empty string",
            }

        params: Dict[str, Any] = {
            "knowledgeBaseId": kb_id,
            "retrievalQuery": {"text": query.strip()},
        }
        if retrieval_configuration is not None:
            params["retrievalConfiguration"] = retrieval_configuration
        else:
            params["retrievalConfiguration"] = {
                "vectorSearchConfiguration": {
                    "numberOfResults": number_of_results,
                }
            }
        if next_token:
            params["nextToken"] = next_token

        try:
            response = self.client.retrieve(**params)
            results = self._normalize_retrieval_results(response.get("retrievalResults") or [])
            return {
                "success": True,
                "action": "rag_retrieve",
                "query": query.strip(),
                "results": results,
                "next_token": response.get("nextToken"),
                "raw": response,
            }
        except Exception as e:
            self.logger.error(f"rag_retrieve failed: {e}")
            return {
                "success": False,
                "action": "rag_retrieve",
                "query": query.strip(),
                "error": str(e),
            }

    def rag_generate(
        self,
        query: str,
        *,
        session_id: Optional[str] = None,
        number_of_results: int = 5,
        retrieval_configuration: Optional[Dict[str, Any]] = None,
        generation_configuration: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Retrieve from the Knowledge Base and generate an answer.

        Equivalent to bedrock-agent-runtime RetrieveAndGenerate.
        """
        kb_id = self._require_kb_id()
        model_arn = self._require_model_arn()
        if not isinstance(query, str) or not query.strip():
            return {
                "success": False,
                "action": "rag_generate",
                "error": "query must be a non-empty string",
            }

        kb_config: Dict[str, Any] = {
            "knowledgeBaseId": kb_id,
            "modelArn": model_arn,
        }
        if retrieval_configuration is not None:
            kb_config["retrievalConfiguration"] = retrieval_configuration
        else:
            kb_config["retrievalConfiguration"] = {
                "vectorSearchConfiguration": {
                    "numberOfResults": number_of_results,
                }
            }
        if generation_configuration is not None:
            kb_config["generationConfiguration"] = generation_configuration

        params: Dict[str, Any] = {
            "input": {"text": query.strip()},
            "retrieveAndGenerateConfiguration": {
                "type": "KNOWLEDGE_BASE",
                "knowledgeBaseConfiguration": kb_config,
            },
        }
        if session_id:
            params["sessionId"] = session_id

        try:
            response = self.client.retrieve_and_generate(**params)
            output = response.get("output") or {}
            return {
                "success": True,
                "action": "rag_generate",
                "query": query.strip(),
                "answer": output.get("text", ""),
                "citations": self._normalize_citations(response.get("citations") or []),
                "session_id": response.get("sessionId"),
                "raw": response,
            }
        except Exception as e:
            self.logger.error(f"rag_generate failed: {e}")
            return {
                "success": False,
                "action": "rag_generate",
                "query": query.strip(),
                "error": str(e),
            }
