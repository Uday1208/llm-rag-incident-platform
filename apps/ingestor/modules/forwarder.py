# apps/ingestor/modules/forwarder.py
# HTTP forward helpers to rag-worker /v1/ingest

from typing import List, Dict
import httpx
import json
import logging

log = logging.getLogger("ingestor.forwarder")

def get_http_client(token: str = "") -> httpx.AsyncClient:
    headers = {"content-type":"application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    return httpx.AsyncClient(headers=headers)

async def post_docs(http: httpx.AsyncClient, url: str, docs: List[Dict], timeout: float) -> bool:
    try:
        resp = await http.post(url, json={"documents": docs}, timeout=timeout)
        ok = 200 <= resp.status_code < 300
        if ok:
            log.info(json.dumps({"msg":"rag-worker ingest ok","status":resp.status_code,"count":len(docs)}))
            return True
        body = (await resp.aread())[:256].decode("utf-8", "ignore")
        log.warning(json.dumps({"msg":"rag-worker ingest not ok","status":resp.status_code,"count":len(docs),"body":body}))
        return False
    except Exception as e:
        log.error(json.dumps({"msg":"rag-worker ingest failed","err":str(e),"count":len(docs)}))
        return False
