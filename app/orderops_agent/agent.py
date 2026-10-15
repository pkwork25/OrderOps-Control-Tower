"""OrderOps copilot: LangGraph ReAct agent on Llama, tools via Databricks managed MCP.

Request path:
  user -> input guardrail -> ReAct loop (Llama via Unity AI Gateway <-> MCP UC-function tools)
       -> output guardrail -> user
Every run is an MLflow trace: LangChain/LangGraph autologging captures LLM + tool spans, and the
guardrails are explicit GUARDRAIL spans, tagged with user/session for filtering in the MLflow UI.
"""
from __future__ import annotations

import os
from typing import Any

import mlflow
from databricks.sdk import WorkspaceClient
from langgraph.prebuilt import create_react_agent

from .guardrails import check_input, check_output
from .mcp_tools import load_mcp_tools

SYSTEM_PROMPT = """You are OrderOps Copilot, a read-only operations assistant for an e-commerce order control tower.
You answer questions about orders, SLA breaches, customers, KPIs and CDC pipeline health.

Rules:
- ALWAYS call a tool for any fact about orders, amounts, statuses, counts or freshness. Never guess or invent IDs or numbers.
- Order IDs look like ORD-1A2B3C4D; customer IDs look like CUST-00042. Regions: North, South, East, West (use ALL for every region).
- If get_order returns nothing, call get_order_history - the order may have been deleted or cancelled - and say so plainly.
- If a tool returns no rows, say that clearly instead of speculating.
- You are read-only. You cannot cancel, refund, update or delete anything; say so if asked.
- Amounts are in INR (use the ₹ symbol). Show 4+ rows as a compact markdown table. Be brief and operational.
- If the user asks whether data is current, call pipeline_freshness and report the lag in minutes."""

_traced_check_input = mlflow.trace(check_input, name="input_guardrail", span_type="GUARDRAIL")
_traced_check_output = mlflow.trace(check_output, name="output_guardrail", span_type="GUARDRAIL")


def build_llm(ws: WorkspaceClient):
    """Prefer the Unity AI Gateway route; fall back to the raw Foundation Model API endpoint."""
    gateway_model = os.getenv("LLM_GATEWAY_MODEL", "").strip()
    if gateway_model:
        import httpx
        from langchain_openai import ChatOpenAI

        def _databricks_auth(request: httpx.Request) -> None:
            # Fresh OAuth header per request: the app's service-principal token rotates hourly.
            request.headers.update(ws.config.authenticate())

        return ChatOpenAI(
            model=gateway_model,  # e.g. main.orderops.orderops_llm
            base_url=f"{ws.config.host.rstrip('/')}/ai-gateway/mlflow/v1",
            api_key="databricks-oauth",  # placeholder, overwritten by the hook above
            http_client=httpx.Client(event_hooks={"request": [_databricks_auth]}, timeout=120),
            temperature=0,
            max_tokens=1024,
        )

    from databricks_langchain import ChatDatabricks
    return ChatDatabricks(endpoint=os.environ.get("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"),
                          temperature=0, max_tokens=1024)


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):  # some providers return content blocks
        return "".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in content)
    return str(content)


class OrderOpsCopilot:
    def __init__(self, catalog: str, schema: str, workspace_client: WorkspaceClient | None = None):
        self.ws = workspace_client or WorkspaceClient()
        host = self.ws.config.host.rstrip("/")
        self.mcp_urls = [f"{host}/api/2.0/mcp/functions/{catalog}/{schema}"]
        self.tools = load_mcp_tools(self.mcp_urls, self.ws)
        self.llm = build_llm(self.ws)
        self.graph = create_react_agent(self.llm, self.tools, prompt=SYSTEM_PROMPT)

    @property
    def tool_names(self) -> list[str]:
        return [t.name.split("__")[-1] for t in self.tools]

    @mlflow.trace(name="orderops_copilot", span_type="AGENT")
    def ask(self, question: str, history: list[dict] | None = None,
            user: str | None = None, session_id: str | None = None) -> dict:
        try:
            mlflow.update_current_trace(
                metadata={k: v for k, v in {"mlflow.trace.user": user, "mlflow.trace.session": session_id}.items() if v},
                tags={"app": "orderops-copilot"},
            )
        except Exception:
            pass  # tracing must never break the answer path

        gin = _traced_check_input(question)
        if not gin.allowed:
            return {"answer": gin.message, "blocked": True, "guardrails": gin.reasons, "tools_used": []}

        messages = list(history or [])[-6:] + [{"role": "user", "content": gin.text}]
        state = self.graph.invoke({"messages": messages}, config={"recursion_limit": 12})

        tool_msgs = [m for m in state["messages"] if getattr(m, "type", "") == "tool"]
        final = _text(state["messages"][-1].content)
        gout = _traced_check_output(final, evidence=[_text(m.content) for m in tool_msgs] + [gin.text],
                                    system_prompt=SYSTEM_PROMPT)
        return {
            "answer": gout.text if gout.allowed else gout.message,
            "blocked": not gout.allowed,
            "guardrails": gin.reasons + gout.reasons,
            "tools_used": [getattr(m, "name", "tool").split("__")[-1] for m in tool_msgs],
        }
