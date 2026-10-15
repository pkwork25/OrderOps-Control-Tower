import sys
from pathlib import Path

# The agent package lives inside app/ so Databricks Apps ships it with the Streamlit app
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
