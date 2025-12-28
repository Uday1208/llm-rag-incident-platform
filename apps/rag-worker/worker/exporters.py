"""
Export helpers for fine-tuning data.
Converts incident bundles and resolutions into OpenAI-compatible JSONL format.
"""
from typing import List, Dict, Any, Optional
import json

SYSTEM_PROMPT = """You are an expert Site Reliability Engineer (SRE). 
Given an incident bundle containing logs, symptoms, and service details, provide a structured technical resolution including summary, root cause, and actionable steps."""


def format_for_finetuning(bundle: Dict, resolution: Dict) -> Dict[str, Any]:
    """
    Convert a bundle and its resolution into a single OpenAI training example.
    
    Format:
    - System: SRE persona
    - User: Incident details (Service, Severity, Logs, Symptoms)
    - Assistant: Resolution (Summary, Root Cause, Steps)
    """
    # 1. Construct User Message (Input)
    user_parts = [
        f"Service: {bundle.get('service', 'unknown')}",
        f"Severity: {bundle.get('severity', 'unknown')}",
        f"TraceID: {bundle.get('trace_id', 'unknown')}",
    ]
    
    if bundle.get('symptoms'):
        user_parts.append(f"\nSymptoms:\n{bundle['symptoms']}")
        
    if bundle.get('error_signature'):
        user_parts.append(f"\nError Signature:\n{bundle['error_signature']}")
        
    # Add logs (truncate if too long, though fine-tuning allows large context)
    content = bundle.get('content', '')
    if content:
        user_parts.append(f"\nLogs:\n{content}")
        
    user_content = "\n".join(user_parts)

    # 2. Construct Assistant Message (Target Output)
    assistant_parts = [
        f"Summary: {resolution.get('summary', '')}"
    ]
    
    if resolution.get('root_cause'):
        assistant_parts.append(f"\nRoot Cause: {resolution.get('root_cause')}")
        
    if resolution.get('steps'):
        # Format steps nicely
        steps = resolution['steps']
        if isinstance(steps, str):
            assistant_parts.append(f"\nSteps:\n{steps}")
        elif isinstance(steps, list):
            assistant_parts.append("\nSteps:")
            for i, step in enumerate(steps, 1):
                assistant_parts.append(f"{i}. {step}")
        elif isinstance(steps, dict):
             assistant_parts.append(f"\nSteps:\n{json.dumps(steps, indent=2)}")

    assistant_content = "\n\n".join(assistant_parts)

    return {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
            {"role": "assistant", "content": assistant_content}
        ]
    }
