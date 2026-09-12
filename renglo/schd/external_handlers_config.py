"""
Configuration for extensions with external handlers (Lambda functions)

This module manages configuration for extensions that have handlers deployed
as external Lambda functions vs internal handlers loaded via SchdLoader.

Configuration can be:
1. Centralized in this file (for system-wide config)
2. Per-extension config files (discovered automatically)
3. Environment variables

Configuration Loading Order:
    1. Environment variables: EXTERNAL_HANDLERS (production - convention-based)
    2. Environment variables: {EXTENSION_NAME}_EXTERNAL_HANDLERS_{FIELD} (legacy support)
    3. File system: extensions/{extension_name}/extension_config.json (development only)
    4. Default config: DEFAULT_CONFIG in this file

IMPORTANT: This is SYSTEM-LEVEL configuration, not package-level.
The system Lambda needs to know which extensions have external handlers
and how to call them. The extension package itself doesn't need this config
(it's installed in a different Lambda).

Conventions (if extension is in EXTERNAL_HANDLERS list):
    - Lambda function name: {extension}-handlers
    - Lambda region: Same as system Lambda (from AWS_REGION)
    - Docker image: {extension}-lambda-builder:latest
    - Enabled: true (if in list)
    - Active: true (if in list)

For Production (Lambda - System):
    - Simple: Set EXTERNAL_HANDLERS in zappa_settings.json environment_variables:
        "EXTERNAL_HANDLERS": "arbitium,other-extension"
    
    - Or use individual vars (legacy):
        "ARBITIUM_EXTERNAL_HANDLERS_ENABLED": "true",
        "ARBITIUM_EXTERNAL_HANDLERS_ACTIVE": "true",
        "ARBITIUM_EXTERNAL_HANDLERS_LAMBDA_FUNCTION": "arbitium-handlers",
        "ARBITIUM_EXTERNAL_HANDLERS_LAMBDA_REGION": "us-east-1",
        "ARBITIUM_EXTERNAL_HANDLERS_DOCKER_IMAGE": "arbitium-lambda-builder:latest"

For Development:
    - Add to dev/renglo-api/env_config.py (or set RENGLO_CONFIG_PATH):
        EXTERNAL_HANDLERS = 'arbitium'
    
    This uses the same mechanism as other environment variables and works
    automatically when load_config() reads env_config.py.
"""

import json
import os
import importlib
from pathlib import Path
from typing import Dict, Any, Optional


def _get_workspace_root() -> Optional[Path]:
    """Resolve workspace root (repo root) from this module's path. Walks up until a dir has both extensions/ and dev/."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "extensions").is_dir() and (current / "dev").is_dir():
            return current
        current = current.parent
    return None


def _get_default_vpc_network_config(region: str) -> Dict[str, Any]:
    """
    Get default VPC subnets and default security group for Fargate.
    Returns {"subnets": [...], "security_groups": [...]}; partial or empty on failure.
    """
    result: Dict[str, Any] = {"subnets": [], "security_groups": []}
    try:
        import boto3
        ec2 = boto3.client("ec2", region_name=region)
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}]).get("Vpcs", [])
        if not vpcs:
            return result
        vpc_id = vpcs[0]["VpcId"]
        subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])\
            .get("Subnets", [])
        result["subnets"] = [s["SubnetId"] for s in subnets if s.get("SubnetId")]
        sgs = ec2.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": ["default"]},
            ]
        ).get("SecurityGroups", [])
        if sgs and sgs[0].get("GroupId"):
            result["security_groups"] = [sgs[0]["GroupId"]]
    except Exception:
        pass
    return result


# Default configuration structure
DEFAULT_CONFIG = {
    "extensions": {
        # Example: "arbitium": {
        #     "has_external_handlers": True,
        #     "active": True,
        #     "lambda_function_name": "arbitium-handlers",
        #     "lambda_region": "us-east-1",
        #     "docker_image": "arbitium-lambda-builder:latest",
        #     "package_path": "extensions/arbitium/package",  # Optional - auto-detected if not provided
        # }
    }
}


def _function_name_from_lambda_arn(arn: str) -> str:
    """Extract function name from arn:aws:lambda:...:function:name[:qualifier]."""
    arn = arn.strip()
    if ":function:" not in arn:
        return arn
    tail = arn.split(":function:", 1)[1]
    return tail.split(":")[0] if tail else arn


def _resolve_handlers_lambda_function_name(extension_name: str) -> str:
    """
    Resolve handlers Lambda name for invoke.

    Priority (backend env):
      1. LAMBDA_EXTERNAL_HANDLERS_ARN
      2. LAMBDA_HANDLERS_FUNCTION_NAME
      3. {extension_name}-handlers (legacy convention)
    """
    arn = (os.getenv("LAMBDA_EXTERNAL_HANDLERS_ARN") or "").strip()
    if arn:
        return _function_name_from_lambda_arn(arn)
    explicit = (os.getenv("LAMBDA_HANDLERS_FUNCTION_NAME") or "").strip()
    if explicit:
        return explicit
    return f"{extension_name}-handlers"


def _handlers_image_stem_from_function_name(fn: str) -> str:
    name = (fn or "").strip()
    if name.endswith("-handlers"):
        return name[: -len("-handlers")] or name
    return name


def _external_handlers_names() -> list[str]:
    raw = os.getenv("EXTERNAL_HANDLERS", "")
    if not raw:
        try:
            from renglo.common import load_config
            raw = load_config().get("EXTERNAL_HANDLERS", "") or ""
        except Exception:
            raw = ""
    return [ext.strip().lower() for ext in str(raw).split(",") if ext.strip()]


def resolve_handlers_docker_image_base(extension_name: str) -> str:
    """Shared Docker image base for all external handlers (mirrors shared Lambda ARN).

    Priority:
      1. Stem of LAMBDA_EXTERNAL_HANDLERS_ARN / LAMBDA_HANDLERS_FUNCTION_NAME
      2. First name in EXTERNAL_HANDLERS
      3. Legacy {call_extension}-lambda-builder
    """
    arn = (os.getenv("LAMBDA_EXTERNAL_HANDLERS_ARN") or "").strip()
    if arn:
        stem = _handlers_image_stem_from_function_name(_function_name_from_lambda_arn(arn))
        if stem:
            return f"{stem}-lambda-builder"
    explicit = (os.getenv("LAMBDA_HANDLERS_FUNCTION_NAME") or "").strip()
    if explicit:
        stem = _handlers_image_stem_from_function_name(explicit)
        if stem:
            return f"{stem}-lambda-builder"
    names = _external_handlers_names()
    if names:
        return f"{names[0]}-lambda-builder"
    return f"{extension_name}-lambda-builder"


def resolve_handlers_ecs_image_base(extension_name: str) -> str:
    """ECS image base paired with resolve_handlers_docker_image_base."""
    base = resolve_handlers_docker_image_base(extension_name)
    if base.endswith("-lambda-builder"):
        return f"{base[: -len('-lambda-builder')]}-ecs-builder"
    return f"{extension_name}-ecs-builder"


def prefer_local_docker_tag() -> bool:
    """Linux (default): prefer :local (arm). Windows: prefer :latest (amd64)."""
    return os.name != "nt"


def load_extension_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Load configuration for a specific extension.
    
    This is SYSTEM-LEVEL configuration - the system needs to know which extensions
    have external handlers and how to call them. The extension package itself
    doesn't need this config (it's installed in a different Lambda).
    
    Tries multiple sources in order:
    1. Environment variables (from zappa_settings.json for production, env_config.py for development)
       - Uses EXTERNAL_HANDLERS comma-separated list (convention-based)
       - Or individual {EXTENSION_NAME}_EXTERNAL_HANDLERS_* vars (legacy)
    2. Default config
    
    Conventions (if extension is in EXTERNAL_HANDLERS list):
    - Lambda function name: LAMBDA_EXTERNAL_HANDLERS_ARN, else LAMBDA_HANDLERS_FUNCTION_NAME,
      else {extension}-handlers
    - Lambda region: Same as system Lambda (AWS_REGION)
    - Docker image: shared handlers builder (ARN / EXTERNAL_HANDLERS primary), else {extension}-lambda-builder
    - Enabled: true (if in list)
    - Active: true (if in list)
    
    Args:
        extension_name: Name of the extension
        
    Returns:
        Extension config dict or None if not found
    """
    config = None
    
    # Try 1: Environment variables (production - system-level config)
    # Primary method: EXTERNAL_HANDLERS comma-separated list (convention-based)
    extensions = _external_handlers_names()
    
    if extensions:
        if extension_name.lower() in extensions:
            # Extension is in the list - use conventions
            # Get region from system Lambda's region
            lambda_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
            image_base = resolve_handlers_docker_image_base(extension_name)
            
            config = {
                "has_external_handlers": True,
                "active": True,
                "lambda_function_name": _resolve_handlers_lambda_function_name(extension_name),
                "lambda_region": lambda_region,  # Same as system Lambda
                "docker_image": f"{image_base}:latest",
                "ecs_docker_image": f"{resolve_handlers_ecs_image_base(extension_name)}:latest",
                "extension_name": extension_name
            }
            return config
    
    # Fallback: Individual environment variables (legacy support)
    # Format: {EXTENSION_NAME}_EXTERNAL_HANDLERS_{FIELD}
    env_prefix = f"{extension_name.upper()}_EXTERNAL_HANDLERS_"
    env_enabled = os.getenv(f"{env_prefix}ENABLED", "").lower()
    
    # Check if external handlers are configured via individual env vars
    if env_enabled in ["true", "false"]:
        lambda_region = os.getenv(f"{env_prefix}LAMBDA_REGION") or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        image_base = resolve_handlers_docker_image_base(extension_name)
        default_docker = f"{image_base}:latest"
        config = {
            "has_external_handlers": env_enabled == "true",
            "active": os.getenv(f"{env_prefix}ACTIVE", "true").lower() == "true",
            "lambda_function_name": (
                os.getenv(f"{env_prefix}LAMBDA_FUNCTION")
                or _resolve_handlers_lambda_function_name(extension_name)
            ),
            "lambda_region": lambda_region,
            "docker_image": os.getenv(f"{env_prefix}DOCKER_IMAGE", default_docker),
            "ecs_docker_image": f"{resolve_handlers_ecs_image_base(extension_name)}:latest",
            "extension_name": extension_name
        }
        return config
    
    # Try 2: Check DEFAULT_CONFIG
    if extension_name in DEFAULT_CONFIG.get("extensions", {}):
        default_config = DEFAULT_CONFIG["extensions"][extension_name].copy()
        if 'external_handlers' in default_config:
            default_config = default_config['external_handlers']
        default_config["extension_name"] = extension_name
        return default_config
    
    return None


def has_external_handlers(extension_name: str) -> bool:
    """
    Check if an extension has external handlers configured.
    
    Args:
        extension_name: Name of the extension
        
    Returns:
        True if extension has external handlers, False otherwise
    """
    config = load_extension_config(extension_name)
    return config is not None and config.get("has_external_handlers", False)


def is_external_handler_active(extension_name: str) -> bool:
    """
    Check if external handlers for an extension are active.
    
    Args:
        extension_name: Name of the extension
        
    Returns:
        True if external handlers are active, False if deactivated or not configured
    """
    config = load_extension_config(extension_name)
    if not config:
        return False
    return config.get("active", True) and config.get("has_external_handlers", False)


def get_lambda_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Get Lambda configuration for an extension.
    
    Args:
        extension_name: Name of the extension
        
    Returns:
        Dict with lambda_function_name and lambda_region, or None
    """
    config = load_extension_config(extension_name)
    if not config or not config.get("has_external_handlers", False):
        return None
    
    return {
        "function_name": config.get("lambda_function_name", f"{extension_name}-handlers"),
        "region": config.get("lambda_region", "us-east-1")
    }


def get_local_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Get local Docker configuration for an extension.
    
    Returns docker_image (small/Lambda) and ecs_docker_image (large/ECS).
    Use ecs_docker_image when the handler is in the ECS handlers list.
    
    Args:
        extension_name: Name of the extension
        
    Returns:
        Dict with docker_image, ecs_docker_image, package_path (auto-detected), or None
    """
    config = load_extension_config(extension_name)
    if not config or not config.get("has_external_handlers", False):
        return None
    
    # Auto-detect package_path - it's always extensions/{extension_name}/package
    # Only use config if explicitly provided (for non-standard layouts)
    package_path = config.get("package_path")
    if not package_path:
        package_path = f"extensions/{extension_name}/package"
    
    image_base = resolve_handlers_docker_image_base(extension_name)
    ecs_base = resolve_handlers_ecs_image_base(extension_name)
    return {
        "docker_image": config.get("docker_image", f"{image_base}:latest"),
        "ecs_docker_image": config.get("ecs_docker_image", f"{ecs_base}:latest"),
        "package_path": package_path
    }


def _get_ecs_handlers_list_from_env() -> Dict[str, list]:
    """
    Parse EXTERNAL_HANDLERS_ECS_HANDLERS env/config.
    Format: "ext1:handler1,handler2;ext2:handler3" or "ext1:handler1,handler2"
    Returns dict: { "ext1": ["handler1", "handler2"], "ext2": ["handler3"] }
    """
    raw = os.getenv("EXTERNAL_HANDLERS_ECS_HANDLERS", "")
    if not raw:
        try:
            from renglo.common import load_config
            cfg = load_config()
            raw = cfg.get("EXTERNAL_HANDLERS_ECS_HANDLERS", "") or raw
        except Exception:
            pass
    result = {}
    for part in raw.split(";"):
        part = part.strip()
        if ":" not in part:
            continue
        ext, handlers_str = part.split(":", 1)
        ext = ext.strip().lower()
        handlers = [h.strip().lower() for h in handlers_str.split(",") if h.strip()]
        if ext:
            result[ext] = handlers
    return result


def _get_ecs_handlers_from_package(extension_name: str) -> list:
    """Read ``ecs_handlers`` from extensions/<name>/package/handlers_config.json."""
    root = _get_workspace_root()
    if not root:
        return []
    path = root / "extensions" / extension_name / "package" / "handlers_config.json"
    if not path.is_file():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []
    raw = data.get("ecs_handlers") or []
    if not isinstance(raw, list):
        return []
    return [str(h).strip().lower() for h in raw if str(h).strip()]


def get_ecs_handlers(extension_name: str) -> list:
    """
    Return list of handler names that run on ECS for this extension.
    Prefers package handlers_config.json ``ecs_handlers``; falls back to env string.
    """
    from_pkg = _get_ecs_handlers_from_package(extension_name)
    if from_pkg:
        return from_pkg
    mapping = _get_ecs_handlers_list_from_env()
    return mapping.get(extension_name.lower(), [])


def is_ecs_handler(extension_name: str, handler_name: str) -> bool:
    """
    Return True if this (extension, handler) should run on ECS (large container).
    handler_name can be "helper_iam" or "helper_iam/ls"; we match by base handler name.
    """
    ecs_list = get_ecs_handlers(extension_name)
    if not ecs_list:
        return False
    base = handler_name.split("/")[0].strip().lower()
    return base in ecs_list


def get_ecs_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Get ECS invocation config for an extension (cluster, task definition, S3 bucket, network).
    Used when invoking handlers via ECS run_task + S3 results.
    Reads ECS_* env vars / deploy_input (no laptop ecs_deploy_config.json).
    """
    config = load_extension_config(extension_name)
    if not config or not config.get("has_external_handlers", False):
        return None
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    file_cfg: Dict[str, Any] = {}

    def _str(key: str, env_key: str, default: str = "") -> str:
        return (file_cfg.get(key) or os.getenv(env_key, default)) or ""

    def _list(key: str, env_key: str) -> list:
        from_file = file_cfg.get(key)
        if isinstance(from_file, list) and from_file:
            return [str(x).strip() for x in from_file if x]
        raw = os.getenv(env_key, "")
        return [s.strip() for s in raw.split(",") if s.strip()]

    bucket = _str("s3_bucket", "ECS_RESULTS_BUCKET")
    cluster = _str("cluster", "ECS_CLUSTER")
    task_def = _str("task_definition", "ECS_TASK_DEFINITION") or f"{extension_name}-handlers-ecs"
    launch_type = (
        _str("launch_type", "ECS_LAUNCH_TYPE", "fargate").lower() or "fargate"
    )
    if launch_type not in ("fargate", "ec2"):
        launch_type = "fargate"
    network_mode = (_str("network_mode", "ECS_NETWORK_MODE", "") or "").lower()
    if network_mode not in ("awsvpc", "bridge", "host", ""):
        network_mode = ""

    def _effective_network_mode() -> str:
        if network_mode:
            return network_mode
        return "bridge" if launch_type == "ec2" else "awsvpc"

    eff_net = _effective_network_mode()
    if launch_type == "fargate" and eff_net != "awsvpc":
        eff_net = "awsvpc"

    needs_awsvpc_net = launch_type == "fargate" or (
        launch_type == "ec2" and eff_net == "awsvpc"
    )

    subnets = _list("subnets", "ECS_SUBNETS")
    security_groups = _list("security_groups", "ECS_SECURITY_GROUPS")
    # Auto-fill from default VPC if bucket/cluster are set but network is missing
    if needs_awsvpc_net and bucket and cluster and (not subnets or not security_groups):
        default_net = _get_default_vpc_network_config(region)
        if not subnets and default_net.get("subnets"):
            subnets = default_net["subnets"]
        if not security_groups and default_net.get("security_groups"):
            security_groups = default_net["security_groups"]

    if not bucket or not cluster:
        return None
    if needs_awsvpc_net and (not subnets or not security_groups):
        return None

    return {
        "region": region,
        "s3_bucket": bucket,
        "cluster": cluster,
        "task_definition": task_def,
        "launch_type": launch_type,
        "network_mode": eff_net,
        "subnets": subnets,
        "security_groups": security_groups,
        "payload_prefix": "payloads",
        "result_prefix": "results",
    }


def get_batch_s3_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Get S3 config for batch payload/result storage (bucket, region, prefixes).
    Used by batch start/result/status when running locally (in-process or dev Docker)
    without full ECS. Prefer get_ecs_config when available; otherwise use global
    ECS_RESULTS_BUCKET + AWS_REGION so any handler can run in batch mode.
    """
    cfg = get_ecs_config(extension_name)
    if cfg:
        return {
            "region": cfg["region"],
            "s3_bucket": cfg["s3_bucket"],
            "payload_prefix": cfg.get("payload_prefix", "payloads"),
            "result_prefix": cfg.get("result_prefix", "results"),
        }
    try:
        from renglo.common import load_config
        config = load_config()
    except Exception:
        config = {}
    bucket = (config.get("ECS_RESULTS_BUCKET") or os.getenv("ECS_RESULTS_BUCKET") or "").strip()
    region = (config.get("AWS_REGION") or config.get("AWS_DEFAULT_REGION") or
              os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1")
    if not bucket:
        return None
    return {
        "region": region,
        "s3_bucket": bucket,
        "payload_prefix": "payloads",
        "result_prefix": "results",
    }
