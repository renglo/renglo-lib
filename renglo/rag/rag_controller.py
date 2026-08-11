from __future__ import annotations

import base64
from typing import Any, Dict, List, Optional, Union

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
        self.docs_bucket = (self.config.get("RAG_DOCS_BUCKET") or "").strip()
        self.docs_prefix = (self.config.get("RAG_DOCS_PREFIX") or "rag/").strip()
        if self.docs_prefix and not self.docs_prefix.endswith("/"):
            self.docs_prefix = f"{self.docs_prefix}/"
        self.data_source_id = (self.config.get("RAG_DATA_SOURCE_ID") or "").strip()
        resolved_region = region_name or self.config.get("AWS_REGION", "us-east-1")
        self.region = resolved_region
        self.client = bedrock_client or boto3.client(
            "bedrock-agent-runtime",
            region_name=resolved_region,
        )
        self._s3 = None

    def status(self) -> Dict[str, Any]:
        return {
            "success": True,
            "action": "status",
            "kb_id": self.kb_id or None,
            "rag_model_arn_configured": bool(self.model_arn),
            "rag_docs_bucket": self.docs_bucket or None,
            "rag_docs_prefix": self.docs_prefix or None,
            "rag_data_source_id": self.data_source_id or None,
            "configured": bool(self.kb_id),
        }

    def _s3_client(self):
        if self._s3 is None:
            self._s3 = boto3.client("s3", region_name=self.region)
        return self._s3

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

    def upload_bytes(
        self,
        *,
        filename: str,
        body: Union[bytes, bytearray],
        subpath: str = "runbooks",
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload a document into the RAG docs bucket under the configured prefix."""
        action = "upload_bytes"
        docs_bucket = (bucket or self.docs_bucket or "").strip()
        docs_prefix = (prefix or self.docs_prefix or "rag/").strip()
        if docs_prefix and not docs_prefix.endswith("/"):
            docs_prefix = f"{docs_prefix}/"
        name = str(filename or "").strip().lstrip("/")
        if not docs_bucket:
            return {"success": False, "action": action, "error": "RAG_DOCS_BUCKET is not configured"}
        if not name:
            return {"success": False, "action": action, "error": "filename is required"}
        sub = str(subpath or "runbooks").strip().strip("/")
        key = f"{docs_prefix}{sub}/{name}" if sub else f"{docs_prefix}{name}"
        try:
            put_kwargs: Dict[str, Any] = {
                "Bucket": docs_bucket,
                "Key": key,
                "Body": bytes(body),
            }
            if content_type:
                put_kwargs["ContentType"] = content_type
            self._s3_client().put_object(**put_kwargs)
            return {
                "success": True,
                "action": action,
                "bucket": docs_bucket,
                "key": key,
                "bytes": len(body),
            }
        except Exception as e:
            self.logger.error(f"upload_bytes failed: {e}")
            return {"success": False, "action": action, "error": str(e)}

    def upload_doc(
        self,
        *,
        filename: str,
        content_text: Optional[str] = None,
        content_base64: Optional[str] = None,
        subpath: str = "runbooks",
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload text or base64 content into the RAG docs prefix."""
        action = "upload_doc"
        if content_base64:
            try:
                body = base64.b64decode(str(content_base64))
            except Exception as e:
                return {"success": False, "action": action, "error": f"invalid content_base64: {e}"}
        elif content_text is not None:
            body = str(content_text).encode("utf-8")
        else:
            return {
                "success": False,
                "action": action,
                "error": "content_base64 or content_text is required",
            }
        result = self.upload_bytes(
            filename=filename,
            body=body,
            subpath=subpath,
            bucket=bucket,
            prefix=prefix,
            content_type=content_type,
        )
        if result.get("success"):
            result["action"] = action
        return result

    def start_ingestion_job(
        self,
        *,
        data_source_id: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Start a Bedrock KB data-source sync into the configured vector store (S3 Vectors)."""
        kb_id = self._require_kb_id()
        ds_id = (data_source_id or self.data_source_id or "").strip()
        if not ds_id:
            return {
                "success": False,
                "action": "start_ingestion_job",
                "error": "RAG_DATA_SOURCE_ID (or data_source_id) is required",
            }
        try:
            agent = boto3.client("bedrock-agent", region_name=self.region)
            params: Dict[str, Any] = {
                "knowledgeBaseId": kb_id,
                "dataSourceId": ds_id,
            }
            if description:
                params["description"] = str(description)[:200]
            response = agent.start_ingestion_job(**params)
            job = response.get("ingestionJob") or {}
            return {
                "success": True,
                "action": "start_ingestion_job",
                "knowledge_base_id": kb_id,
                "data_source_id": ds_id,
                "ingestion_job_id": job.get("ingestionJobId"),
                "status": job.get("status"),
                "raw": response,
            }
        except Exception as e:
            self.logger.error(f"start_ingestion_job failed: {e}")
            return {
                "success": False,
                "action": "start_ingestion_job",
                "error": str(e),
            }

    def get_ingestion_job(
        self,
        ingestion_job_id: str,
        *,
        data_source_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        kb_id = self._require_kb_id()
        ds_id = (data_source_id or self.data_source_id or "").strip()
        job_id = str(ingestion_job_id or "").strip()
        if not ds_id or not job_id:
            return {
                "success": False,
                "action": "get_ingestion_job",
                "error": "data_source_id and ingestion_job_id are required",
            }
        try:
            agent = boto3.client("bedrock-agent", region_name=self.region)
            response = agent.get_ingestion_job(
                knowledgeBaseId=kb_id,
                dataSourceId=ds_id,
                ingestionJobId=job_id,
            )
            job = response.get("ingestionJob") or {}
            return {
                "success": True,
                "action": "get_ingestion_job",
                "ingestion_job_id": job.get("ingestionJobId"),
                "status": job.get("status"),
                "statistics": job.get("statistics"),
                "failure_reasons": job.get("failureReasons"),
                "raw": response,
            }
        except Exception as e:
            return {
                "success": False,
                "action": "get_ingestion_job",
                "error": str(e),
            }
