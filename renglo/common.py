import re
import hashlib
import os
import sys
import importlib.util


def get_username_from_email(email):
    # Extract the part before the @
    username = email.split('@')[0]
    # Remove any non-alphanumeric characters
    cleaned_username = re.sub(r'[^a-zA-Z0-9]', '', username)
    return cleaned_username


def create_md5_hash(input_string, num_digits):
    # Create an MD5 hash object
    md5_hash = hashlib.md5()
    # Update the hash object with the input string encoded as bytes
    md5_hash.update(input_string.encode('utf-8'))
    # Get the full hexadecimal MD5 hash
    full_hash = md5_hash.hexdigest()
    # Return the first N digits of the hash
    return full_hash[:num_digits]


_TAG_INJECTION_PATTERN = re.compile(r'[{};:\/\'\"\\\(\)\[\]\$\|&<>]')


def _entity_tag_text_is_safe(text: str) -> bool:
    return not _TAG_INJECTION_PATTERN.search(text)


def _normalize_tag_values(value):
    """Coerce a tag value into a deduplicated list of safe strings."""
    if value is None:
        return []
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple)):
        candidates = value
    else:
        candidates = [value]

    normalized = []
    seen = set()
    for item in candidates:
        if item is None:
            continue
        tag_value = str(item).strip()
        if not tag_value or not _entity_tag_text_is_safe(tag_value):
            continue
        if tag_value in seen:
            continue
        seen.add(tag_value)
        normalized.append(tag_value)
    return normalized


def sanitize_entity_tags(raw_tags):
    """Normalize tags to dict[str, list[str]] for storage on entity documents."""
    if not isinstance(raw_tags, dict):
        return {}
    clean = {}
    for key, value in raw_tags.items():
        if not isinstance(key, str):
            continue
        tag_key = key.strip().lower()
        if not tag_key or not _entity_tag_text_is_safe(tag_key):
            continue

        values = _normalize_tag_values(value)
        if not values:
            continue

        if tag_key in clean:
            existing = clean[tag_key]
            existing_seen = set(existing)
            for tag_value in values:
                if tag_value not in existing_seen:
                    existing.append(tag_value)
                    existing_seen.add(tag_value)
        else:
            clean[tag_key] = values
    return clean


_LOCAL_DEV_CONSOLE_URL = "http://127.0.0.1:5174"


def resolve_invite_fe_base_url(config):
    """
    Console base URL for /invite links in team-invite emails.

    INVITE_FE_BASE_URL overrides FE_BASE_URL when set (local dev console).
    On a non-Lambda API, defaults to the local Vite URL when FE_BASE_URL is
    unset or still points at Amplify — so invite emails work before cloud console deploy.
    """
    cfg = config or {}
    override = (cfg.get("INVITE_FE_BASE_URL") or "").strip().rstrip("/")
    if override:
        return override

    fe_base = (cfg.get("FE_BASE_URL") or "").strip().rstrip("/")
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        return fe_base

    if not fe_base or fe_base == "x" or "amplifyapp.com" in fe_base:
        return _LOCAL_DEV_CONSOLE_URL
    return fe_base


def load_config():
    """
    Load configuration for handlers from env_config.py or environment variables.
    
    Handlers are independent of Flask and need their own way to access config.
    This function is used by handlers in all extensions
    to load the system configuration before initializing controllers.
    
    Loading Strategy:
    1. Try explicit path from RENGLO_CONFIG_PATH
    2. Try local env_config.py (cwd / walk-up)
    3. Try workspace ``dev/renglo-api/env_config.py`` (local development)
    4. Merge environment variables (env vars take precedence)
    
    Returns:
        dict: Configuration dictionary with all uppercase config variables
        
    Usage in handlers:
        from renglo.common import load_config
        
        class MyHandler:
            def __init__(self):
                config = load_config()
                self.DAC = DataController(config=config)
                self.AUC = AuthController(config=config)
    """
    config = {}
    
    # Try multiple paths to find env_config.py
    possible_paths = []

    def _append_workspace_config_paths(root: str) -> None:
        possible_paths.append(os.path.join(root, "env_config.py"))
        possible_paths.append(os.path.join(root, "dev", "renglo-api", "env_config.py"))
        possible_paths.append(os.path.join(root, "renglo-api", "env_config.py"))

    # 1. Explicit config path (preferred for headless runtimes)
    explicit_config_path = os.getenv("RENGLO_CONFIG_PATH")
    if explicit_config_path:
        possible_paths.append(explicit_config_path)

    # 2. Try relative to current working directory
    possible_paths.append(os.path.join(os.getcwd(), "env_config.py"))
    
    # 3. Try to find workspace root by looking for marker directories
    current_dir = os.getcwd()
    while current_dir != os.path.dirname(current_dir):  # Stop at filesystem root
        if os.path.exists(os.path.join(current_dir, "env_config.py")):
            possible_paths.append(os.path.join(current_dir, "env_config.py"))
            break
        # Look for workspace markers
        if any(os.path.exists(os.path.join(current_dir, marker))
               for marker in ("dev", "extensions", "console", "renglo-api")):
            _append_workspace_config_paths(current_dir)
            break
        current_dir = os.path.dirname(current_dir)
    
    # 4. Try relative from this module's location (renglo/common.py)
    # Go up: renglo -> renglo-lib -> dev -> root
    renglo_lib_path = os.path.dirname(os.path.dirname(__file__))
    workspace_root = os.path.dirname(os.path.dirname(renglo_lib_path))
    _append_workspace_config_paths(workspace_root)
    # Also try sibling of renglo-lib when layout is workspace/dev/renglo-lib
    possible_paths.append(os.path.join(os.path.dirname(renglo_lib_path), "renglo-api", "env_config.py"))
    
    env_config = None
    loaded_from = None
    
    seen_paths = set()
    deduped_paths = []
    for candidate in possible_paths:
        if not candidate or candidate in seen_paths:
            continue
        seen_paths.add(candidate)
        deduped_paths.append(candidate)

    # Try to load from each path
    for config_path in deduped_paths:
        if os.path.exists(config_path):
            try:
                spec = importlib.util.spec_from_file_location("env_config", config_path)
                env_config = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(env_config)
                loaded_from = config_path
                break
            except Exception as e:
                print(f"Warning: Failed to load config from {config_path}: {e}", file=sys.stderr)
                continue
    
    if env_config:
        # Extract all uppercase variables (convention for config constants)
        for key in dir(env_config):
            if key.isupper() and not key.startswith('_'):
                config[key] = getattr(env_config, key)
        print(f"Config loaded from file: {loaded_from}")
    else:
        print("Config file not found, using environment variables", file=sys.stderr)
    
    # Load from environment variables (overwrites file-based config)
    # This allows Lambda/production to use environment variables
    env_var_keys = [
        'WL_NAME', 'BASE_URL', 'FE_BASE_URL', 'INVITE_FE_BASE_URL', 'DOC_BASE_URL',
        'FROM_EMAIL',
        'AWS_REGION', 'API_GATEWAY_ARN', 'ROLE_ARN', 'SYS_ENV',
        'DYNAMODB_ENTITY_TABLE', 'DYNAMODB_BLUEPRINT_TABLE', 'DYNAMODB_RINGDATA_TABLE',
        'DYNAMODB_REL_TABLE', 'DYNAMODB_CHAT_TABLE', 'DYNAMODB_SESSION_TABLE', 'DYNAMODB_GRAPH_TABLE',
        'DYNAMODB_SEARCH_TABLE',
        'GRAPH_DB_ENABLED',
        'CSRF_SESSION_KEY', 'SECRET_KEY',
        'COGNITO_REGION', 'COGNITO_USERPOOL_ID', 'COGNITO_APP_CLIENT_ID',
        'COGNITO_CHECK_TOKEN_EXPIRATION',
        'PREVIEW_LAYER', 'S3_BUCKET_NAME',
        'OPENAI_API_KEY', 'WEBSOCKET_CONNECTIONS',
        'ALLOW_DEV_ORIGINS', 'EXTERNAL_HANDLERS',
        'OPENSEARCH_ENDPOINT', 'OPENSEARCH_INDEX', 'OPENSEARCH_REFRESH',
        'KB_ID', 'RAG_MODEL_ARN',
        'GOOGLE_OAUTH_CLIENT_ID', 'GOOGLE_OAUTH_CLIENT_SECRET',
        'GMAIL_OAUTH_REDIRECT_URI', 'OAUTH_STATE_SECRET',
        'RENGLO_INGRESS_SECRET', 'GMAIL_INGRESS_SECRET', 'WHATSAPP_INGRESS_SECRET',
        'WEBHOOK_EDGE_BASE_URL',
    ]
    
    env_loaded_count = 0
    for key in env_var_keys:
        if key in os.environ:
            config[key] = os.environ[key]
            env_loaded_count += 1
    
    if env_loaded_count > 0:
        print(f"Loaded {env_loaded_count} config values from environment variables")
    
    # Validate critical config exists
    critical_keys = ['DYNAMODB_RINGDATA_TABLE', 'DYNAMODB_ENTITY_TABLE']
    missing_keys = [key for key in critical_keys if key not in config]
    
    if missing_keys:
        raise RuntimeError(
            f"Critical configuration missing: {', '.join(missing_keys)}\n"
            "Please set these as environment variables or provide env_config.py "
            "via RENGLO_CONFIG_PATH"
        )
    
    return config