
import os
import toml

def get_streamlit_theme():
    config_path = os.path.expanduser("/app/.streamlit/config.toml")
    with open(config_path, "r") as f:
        config = toml.load(f)
    return config.get("theme", {})
