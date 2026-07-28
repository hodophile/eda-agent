"""ReAct system prompt and the parser that turns raw LLM text into actions.

We force the model to answer with a single JSON object on every turn. Ollama's
OpenAI-compatible endpoint honours `response_format={"type": "json_object"}`,
which makes the output reliable enough to parse deterministically.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Optional

FINAL_ACTION = "FINAL"

SYSTEM_PROMPT = """You are a ReAct (Reason + Act) agent. On every turn you MUST
reply with a single JSON object and nothing else -- no markdown, no commentary.

To call a tool:
{"thought": "<one short sentence of reasoning>", "action": "<ToolName>", "action_input": "<argument>"}

To give the final answer to the user:
{"thought": "<one short sentence of reasoning>", "action": "FINAL", "action_input": "<final answer>"}

Available tools:
- Calculator: evaluates a mathematical expression. The action_input is the raw
  expression, e.g. "5 * 10 + 3".

Rules:
- Output ONLY the JSON object.
- Whenever the user's request needs a calculation, use the Calculator tool first
  and only answer once you have the observation.
- Use action "FINAL" exactly once, when the task is fully answered.
"""


@dataclass
class ToolCall:
    thought: str
    action: str
    action_input: str

    @property
    def is_final(self) -> bool:
        return self.action.upper() == FINAL_ACTION


_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)
_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def extract_json_object(text: str) -> Optional[dict]:
    """Best-effort extraction of the first JSON object from a model response."""
    if not text:
        return None

    m = _FENCE_RE.search(text)
    candidate = m.group(1) if m else None

    if candidate is None:
        m = _OBJECT_RE.search(text)
        candidate = m.group(0) if m else None

    if candidate is None:
        return None

    try:
        obj = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def parse_react(text: str) -> Optional[ToolCall]:
    """Parse raw LLM output into a ToolCall, or None if it is unparseable."""
    obj = extract_json_object(text)
    if obj is None:
        return None
    action = str(obj.get("action", "")).strip()
    if not action:
        return None
    return ToolCall(
        thought=str(obj.get("thought", "")),
        action=action,
        action_input=str(obj.get("action_input", "")),
    )
