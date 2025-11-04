# apps/ingestor/modules/eh_consumer.py
# Event Hub consumer helpers: construct client + decode EventData bodies.

from typing import Any, Dict, List, Tuple
import json

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import EventData

def get_consumer(conn_str: str, hub: str, group: str, logging_enable: bool = False) -> EventHubConsumerClient:
    return EventHubConsumerClient.from_connection_string(
        conn_str=conn_str,
        eventhub_name=hub,
        consumer_group=group,
        logging_enable=logging_enable,
    )

def _raw_body(event: EventData) -> bytes:
    """Support both bytes and generator-of-bytes bodies across SDK versions."""
    body = getattr(event, "body", None)
    if isinstance(body, (bytes, bytearray)):
        return bytes(body)
    # generator/iterable of bytes
    return b"".join(part for part in body)

def decode_event_items(event: EventData) -> Tuple[List[Dict[str, Any]], str]:
    """
    Returns (items, raw_text): list of dicts extracted from JSON/JSONL payload + raw text.
    Understands diagnostics envelopes: {"records":[...]}.
    """
    raw = _raw_body(event)
    text = raw.decode("utf-8", errors="ignore").strip()
    items: List[Dict[str, Any]] = []

    if not text:
        return (items, "")

    try:
        if "\n" in text:
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict) and "records" in obj and isinstance(obj["records"], list):
                        items.extend([r for r in obj["records"] if isinstance(r, dict)])
                    elif isinstance(obj, dict):
                        items.append(obj)
                    else:
                        items.append({"message": str(obj)})
                except Exception:
                    items.append({"message": line})
        else:
            obj = json.loads(text)
            if isinstance(obj, dict) and "records" in obj and isinstance(obj["records"], list):
                items.extend([r for r in obj["records"] if isinstance(r, dict)])
            elif isinstance(obj, dict):
                items.append(obj)
            else:
                items.append({"message": str(obj)})
    except Exception:
        # On parse error, keep the whole thing as a message
        items = [{"message": text}]

    return (items, text + "\n")
