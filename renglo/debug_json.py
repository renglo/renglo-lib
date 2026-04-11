import json
import os
import logging


def djson(filename: str, data):
    """Writes a debug snapshot as JSON. Controlled by DEBUG_JSON env var."""
    if os.environ.get("DEBUG_JSON", "true").lower() != "true":
        return
    debug_dir = os.environ.get("DEBUG_DIR", "debug")
    os.makedirs(debug_dir, exist_ok=True)
    path = os.path.join(debug_dir, filename)
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception as e:
        logging.getLogger("agent").warning("djson failed for %s: %s", filename, e)
