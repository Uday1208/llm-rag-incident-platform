import json
from typing import List, Dict, Any

def parse_log_content(content: str) -> List[Dict[str, Any]]:
    """
    Parse log content in various formats.
    
    Supports:
    - JSONL (newline-delimited JSON)
    - Single JSON object
    - Azure export format ({"records": [...]})
    """
    content = content.strip()
    if not content:
        return []
    
    records = []
    
    # Try JSONL first (most common for streaming exports)
    if content.startswith("{") and "\n" in content:
        for idx, line in enumerate(content.splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    # Handle Azure export envelope
                    if "records" in obj and isinstance(obj["records"], list):
                        for r in obj["records"]:
                            r["_line_index"] = idx
                        records.extend(obj["records"])
                    else:
                        obj["_line_index"] = idx
                        records.append(obj)
                elif isinstance(obj, list):
                    for r in obj:
                        if isinstance(r, dict):
                            r["_line_index"] = idx
                    records.extend(obj)
            except json.JSONDecodeError:
                continue
        
        if records:
            return records
    
    # Try single JSON
    try:
        obj = json.loads(content)
        if isinstance(obj, dict):
            if "records" in obj and isinstance(obj["records"], list):
                for idx, r in enumerate(obj["records"], 1):
                    r["_line_index"] = idx
                return obj["records"]
            obj["_line_index"] = 1
            return [obj]
        if isinstance(obj, list):
            for idx, r in enumerate(obj, 1):
                if isinstance(r, dict):
                    r["_line_index"] = idx
            return obj
    except json.JSONDecodeError:
        pass
    
    return records
