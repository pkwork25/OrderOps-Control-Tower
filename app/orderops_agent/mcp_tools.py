"""Bridge Databricks managed MCP servers -> LangChain tools.

The UC-functions MCP server (`/api/2.0/mcp/functions/<catalog>/<schema>`) lists every function in
the schema as an MCP tool with a JSON Schema for its inputs. We turn each one into a LangChain
`BaseTool` with a pydantic args model, so LangGraph's ReAct agent can bind them to the LLM.
Auth flows through the WorkspaceClient (the app's service principal on Databricks Apps), so UC
grants decide which tools and rows the copilot can reach.
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, create_model

try:  # langchain is only needed at runtime; schema helpers stay testable without it
    from langchain_core.tools import BaseTool
except ImportError:  # pragma: no cover
    BaseTool = object  # type: ignore

MAX_TOOL_OUTPUT_CHARS = 12_000
_JSON_TO_PY = {"string": str, "integer": int, "number": float, "boolean": bool, "array": list, "object": dict}


def json_schema_to_model(tool_name: str, schema: dict | None) -> type[BaseModel]:
    """Build a pydantic model from an MCP tool's inputSchema (flat object schemas, as UC functions emit)."""
    schema = schema or {}
    props: dict[str, dict] = schema.get("properties") or {}
    required = set(schema.get("required") or [])
    fields: dict[str, Any] = {}
    for name, spec in props.items():
        py_type = _JSON_TO_PY.get(spec.get("type", "string"), str)
        desc = spec.get("description", "")
        if name in required:
            fields[name] = (py_type, Field(..., description=desc))
        else:
            fields[name] = (Optional[py_type], Field(None, description=desc))
    safe = "".join(ch if ch.isalnum() else "_" for ch in tool_name)
    return create_model(f"{safe}_Args", **fields)


def mcp_result_to_text(result: Any) -> str:
    """CallToolResult.content is a list of content blocks; keep the text ones."""
    parts = [getattr(c, "text", None) for c in (getattr(result, "content", None) or [])]
    text = "\n".join(p for p in parts if p) or "(tool returned no rows)"
    if getattr(result, "isError", False):
        text = "TOOL ERROR: " + text
    if len(text) > MAX_TOOL_OUTPUT_CHARS:
        text = text[:MAX_TOOL_OUTPUT_CHARS] + "\n...[truncated - ask a narrower question]"
    return text


class MCPTool(BaseTool):  # type: ignore[misc, valid-type]
    """A LangChain tool that forwards calls to one tool on a Databricks MCP server."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    description: str
    args_schema: type[BaseModel]
    mcp_client: Any = Field(exclude=True)

    def _run(self, **kwargs: Any) -> str:
        args = {k: v for k, v in kwargs.items() if v is not None}
        return mcp_result_to_text(self.mcp_client.call_tool(self.name, args))


def load_mcp_tools(server_urls: list[str], workspace_client: Any) -> list[MCPTool]:
    from databricks_mcp import DatabricksMCPClient  # runtime-only import

    tools: list[MCPTool] = []
    for url in server_urls:
        client = DatabricksMCPClient(server_url=url, workspace_client=workspace_client)
        for t in client.list_tools():
            tools.append(MCPTool(
                name=t.name,
                description=(t.description or t.name)[:1024],
                args_schema=json_schema_to_model(t.name, t.inputSchema),
                mcp_client=client,
            ))
    if not tools:
        raise RuntimeError(f"No MCP tools found at {server_urls}. Did the CDC job's register_mcp_tools task run, "
                           "and does the app's service principal have USE SCHEMA + EXECUTE?")
    return tools
