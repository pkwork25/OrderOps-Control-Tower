"""OrderOps Control Tower - Streamlit app on Databricks Apps.

Left: live KPIs straight from the CDC silver table (SQL warehouse).
Right: the ops copilot (LangGraph ReAct agent -> MCP tools), traced in MLflow.
"""
import os
import uuid

import mlflow
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

st.set_page_config(page_title="OrderOps Control Tower", page_icon="📦", layout="wide")

CATALOG = os.getenv("ORDEROPS_CATALOG", "main")
SCHEMA = os.getenv("ORDEROPS_SCHEMA", "orderops")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
FQ = f"`{CATALOG}`.`{SCHEMA}`"


@st.cache_resource
def workspace() -> WorkspaceClient:
    return WorkspaceClient()


@st.cache_resource
def init_tracing() -> str | None:
    mlflow.set_tracking_uri("databricks")
    exp = os.getenv("MLFLOW_EXPERIMENT_NAME", "/Shared/orderops-copilot-traces")
    try:
        mlflow.set_experiment(exp)
        mlflow.langchain.autolog()
        return exp
    except Exception as e:  # tracing is observability, not a hard dependency
        st.sidebar.warning(f"MLflow tracing disabled: {e}")
        return None


@st.cache_resource(show_spinner="Connecting to MCP tools…")
def copilot():
    from orderops_agent.agent import OrderOpsCopilot
    return OrderOpsCopilot(CATALOG, SCHEMA, workspace())


@st.cache_data(ttl=60, show_spinner=False)
def run_sql(sql: str) -> pd.DataFrame:
    resp = workspace().statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="30s")
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(resp.status.error.message if resp.status.error else resp.status.state)
    cols = [c.name for c in resp.manifest.schema.columns]
    return pd.DataFrame((resp.result.data_array or []) if resp.result else [], columns=cols)


def current_user() -> str:
    # Databricks Apps forwards the signed-in user's identity in request headers
    try:
        return st.context.headers.get("X-Forwarded-Email") or "unknown"
    except Exception:
        return "unknown"


init_tracing()
st.title("📦 OrderOps Control Tower")
st.caption(f"CDC-fed order operations · `{CATALOG}.{SCHEMA}` · Llama via Unity AI Gateway · MCP tools")

kpi_col, chat_col = st.columns([2, 3], gap="large")

# ---------------------------------------------------------------- KPIs
with kpi_col:
    st.subheader("Live operations")
    try:
        k = run_sql(f"""
            SELECT count_if(status NOT IN ('DELIVERED','CANCELLED'))                                   AS open_orders,
                   count_if(status NOT IN ('DELIVERED','CANCELLED') AND promised_delivery_ts < current_timestamp()) AS sla_breaches,
                   coalesce(sum(order_amount) FILTER (WHERE created_ts >= current_date()), 0)          AS gmv_today,
                   timestampdiff(MINUTE, max(_cdc_commit_ts), current_timestamp())                     AS lag_min
            FROM {FQ}.orders_silver""").iloc[0]
        m1, m2 = st.columns(2)
        m1.metric("Open orders", f"{int(k.open_orders or 0):,}")
        m2.metric("SLA breaches", f"{int(k.sla_breaches or 0):,}")
        m3, m4 = st.columns(2)
        m3.metric("GMV today", f"₹{float(k.gmv_today or 0):,.0f}")
        m4.metric("CDC lag", f"{k.lag_min or '–'} min")

        by_status = run_sql(f"SELECT status, sum(orders) AS orders FROM {FQ}.order_ops_kpis GROUP BY status")
        if not by_status.empty:
            by_status["orders"] = by_status["orders"].astype(int)
            st.bar_chart(by_status.set_index("status"), horizontal=True)

        st.markdown("**Most overdue**")
        st.dataframe(run_sql(f"""SELECT order_id, region, status, hours_overdue
                                 FROM {FQ}.list_sla_breaches('ALL') LIMIT 8"""),
                     hide_index=True, use_container_width=True)
    except Exception as e:
        st.error(f"Couldn't load KPIs - has the CDC job run yet? ({e})")

# ---------------------------------------------------------------- Copilot
with chat_col:
    st.subheader("Ops copilot")
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.messages = []

    starters = ["Which West orders are breaching SLA?", "Summarise today's order status across regions",
                "Is the order data up to date?"]
    picked = None
    cols = st.columns(len(starters))
    for c, s in zip(cols, starters):
        if c.button(s, use_container_width=True):
            picked = s

    box = st.container(height=520)
    for m in st.session_state.messages:
        with box.chat_message(m["role"]):
            st.markdown(m["content"])
            if m.get("meta"):
                st.caption(m["meta"])

    prompt = st.chat_input("Ask about an order, customer, SLA or the pipeline…") or picked
    if prompt:
        history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages]
        st.session_state.messages.append({"role": "user", "content": prompt})
        with box.chat_message("user"):
            st.markdown(prompt)
        with box.chat_message("assistant"):
            with st.spinner("Querying live order data…"):
                try:
                    out = copilot().ask(prompt, history=history, user=current_user(),
                                        session_id=st.session_state.session_id)
                except Exception as e:
                    out = {"answer": f"Something went wrong calling the copilot: `{e}`", "tools_used": [],
                           "guardrails": ["error"], "blocked": True}
            st.markdown(out["answer"])
            bits = []
            if out["tools_used"]:
                bits.append("🔧 " + ", ".join(dict.fromkeys(out["tools_used"])))
            if out["guardrails"]:
                bits.append("🛡️ " + ", ".join(out["guardrails"]))
            try:
                tid = mlflow.get_last_active_trace_id()
                if tid:
                    bits.append(f"trace `{tid}`")
            except Exception:
                pass
            meta = " · ".join(bits)
            if meta:
                st.caption(meta)
        st.session_state.messages.append({"role": "assistant", "content": out["answer"], "meta": meta})
