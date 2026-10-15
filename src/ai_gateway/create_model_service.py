# Databricks notebook source
# MAGIC %md
# MAGIC # Unity AI Gateway · model service for the copilot
# MAGIC Creates `<catalog>.<schema>.orderops_llm`, a governed Unity AI Gateway **model service** whose
# MAGIC primary destination is the Llama pay-per-token Foundation Model API endpoint.
# MAGIC The app then calls the gateway's OpenAI-compatible route
# MAGIC (`https://<workspace>/ai-gateway/mlflow/v1`, `model="<catalog>.<schema>.orderops_llm"`) instead
# MAGIC of the raw endpoint, so every LLM call gets the gateway's rate limits, guardrails,
# MAGIC usage tracking and inference logging - and the model behind it can be swapped
# MAGIC (or a fallback added) without redeploying the app.
# MAGIC
# MAGIC **After this runs, finish in the UI** (AI Gateway -> `orderops_llm`), because gateway guardrails and
# MAGIC rate limits are configured there:
# MAGIC * Guardrails: *Jailbreak* (input, block) · *PII redaction* (input + output, sanitize) ·
# MAGIC   *Unsafe content* (output, block)
# MAGIC * Rate limit: e.g. 60 requests/min per user · Inference logging -> this schema · usage tracking on
# MAGIC
# MAGIC > Unity AI Gateway model services are a newer (Beta) API. If the REST shape below changes,
# MAGIC > create the service in the UI: **AI Gateway -> Create -> Model service**, same name and destination.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

for name, default in [("catalog", "main"), ("schema", "orderops"),
                      ("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct"), ("app_sp", "")]:
    dbutils.widgets.text(name, default)

catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
llm_endpoint, app_sp = dbutils.widgets.get("llm_endpoint"), dbutils.widgets.get("app_sp").strip()
SERVICE = "orderops_llm"
w = WorkspaceClient()

body = {
    "comment": "OrderOps copilot LLM route (Llama via Foundation Model APIs), governed by Unity AI Gateway",
    "config": {
        "routing": {
            "destinations": [{
                "name": "primary",
                "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                "pay_per_token_config": {"model": f"models/system.ai.{llm_endpoint}"},
                "traffic_percentage": 100,
            }]
        }
    },
}

try:
    resp = w.api_client.do(
        "POST", "/api/2.1/unity-catalog/model-services",
        query={"parent": f"schemas/{catalog}.{schema}", "model_service_id": SERVICE},
        body=body,
    )
    print("Created model service:", resp)
except Exception as e:
    msg = str(e)
    if "ALREADY_EXISTS" in msg or "already exists" in msg.lower():
        print(f"{catalog}.{schema}.{SERVICE} already exists - leaving it as is.")
    else:
        raise

# COMMAND ----------

# The app's service principal needs EXECUTE on the service (plus USE CATALOG/SCHEMA from notebook 00).
# Model services can't be granted via SQL, so use the UC permissions API.
if app_sp:
    try:
        w.api_client.do(
            "PATCH", f"/api/2.1/unity-catalog/permissions/model_service/{catalog}.{schema}.{SERVICE}",
            body={"changes": [{"principal": app_sp, "add": ["EXECUTE"]}]},
        )
        print("Granted EXECUTE to", app_sp)
    except Exception as e:
        print("Grant via API failed - grant EXECUTE in Catalog Explorer instead:", str(e).splitlines()[0])

print(f"\nSet in app/app.yaml ->  LLM_GATEWAY_MODEL = {catalog}.{schema}.{SERVICE}")
