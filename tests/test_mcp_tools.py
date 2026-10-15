from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from orderops_agent.mcp_tools import MAX_TOOL_OUTPUT_CHARS, json_schema_to_model, mcp_result_to_text

UC_TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "order_id_in": {"type": "string", "description": "Exact order ID"},
        "limit": {"type": "integer", "description": "optional cap"},
    },
    "required": ["order_id_in"],
}


def test_schema_to_model_required_and_optional():
    Model = json_schema_to_model("main__orderops__get_order", UC_TOOL_SCHEMA)
    m = Model(order_id_in="ORD-1A2B3C4D")
    assert m.order_id_in == "ORD-1A2B3C4D" and m.limit is None
    assert Model.model_fields["order_id_in"].description == "Exact order ID"
    with pytest.raises(ValidationError):
        Model()


def test_schema_to_model_no_args():
    Model = json_schema_to_model("main__orderops__pipeline_freshness", {"type": "object", "properties": {}})
    assert Model().model_dump() == {}


def test_result_to_text_joins_and_truncates():
    res = SimpleNamespace(content=[SimpleNamespace(text="a"), SimpleNamespace(text="b")], isError=False)
    assert mcp_result_to_text(res) == "a\nb"
    big = SimpleNamespace(content=[SimpleNamespace(text="x" * (MAX_TOOL_OUTPUT_CHARS + 50))], isError=False)
    assert mcp_result_to_text(big).endswith("narrower question]")
    err = SimpleNamespace(content=[SimpleNamespace(text="boom")], isError=True)
    assert mcp_result_to_text(err).startswith("TOOL ERROR")
    assert mcp_result_to_text(SimpleNamespace(content=[], isError=False)) == "(tool returned no rows)"
