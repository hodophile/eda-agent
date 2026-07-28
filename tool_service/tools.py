"""Tools available to the agent.

Phase 1 ships a single mock tool (Calculator) implemented with a safe AST
evaluator so we can prove the loop mechanics without coupling to external APIs.
New tools are added by extending the TOOL_REGISTRY below.
"""
from __future__ import annotations

import ast
import operator
import re

_BINOPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARYOPS = {ast.UAdd: operator.pos, ast.USub: operator.neg}


def _eval(node: ast.AST) -> float | int:
    if isinstance(node, ast.Expression):
        return _eval(node.body)
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _BINOPS:
        return _BINOPS[type(node.op)](_eval(node.left), _eval(node.right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARYOPS:
        return _UNARYOPS[type(node.op)](_eval(node.operand))
    raise ValueError(f"unsupported expression: {ast.dump(node)}")


def calculator(expression: str) -> str:
    """Evaluate a math expression safely. Returns the result as a string."""
    expr = expression.strip()
    if not expr:
        raise ValueError("empty expression")
    result = _eval(ast.parse(expr, mode="eval"))
    # Drop a trailing ".0" for integer-valued floats for cleaner observations.
    if isinstance(result, float) and result.is_integer():
        return str(int(result))
    return str(result)


# name -> callable(input: str) -> str
TOOL_REGISTRY = {
    "Calculator": calculator,
}

KNOWN_TOOLS_RE = re.compile(r"\b(" + "|".join(TOOL_REGISTRY) + r")\b", re.IGNORECASE)
