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
    - Add to system/env_config.py:
        EXTERNAL_HANDLERS = 'arbitium'
    
    This uses the same mechanism as other environment variables and works
    automatically when the system loads env_config.py.
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


def _load_ecs_deploy_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Load ECS deploy config from extensions/<name>/installer/service/ecs_deploy_config.json
    if present. Written by deploy_ecs.sh. Keys: s3_bucket, cluster, task_definition, subnets[], security_groups[].
    """
    root = _get_workspace_root()
    if not root:
        return None
    path = root / "extensions" / extension_name / "installer" / "service" / "ecs_deploy_config.json"
    if not path.is_file():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
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
    - Lambda function name: {extension}-handlers
    - Lambda region: Same as system Lambda (AWS_REGION)
    - Docker image: {extension}-lambda-builder:latest
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
    # Check os.environ first, then load_config() (which reads env_config.py or env vars)
    external_handlers_list = os.getenv("EXTERNAL_HANDLERS", "")
    if not external_handlers_list:
        # Try to get from load_config() (reads from env_config.py or environment variables)
        try:
            from renglo.common import load_config
            config = load_config()
            external_handlers_list = config.get('EXTERNAL_HANDLERS', '') or external_handlers_list
        except Exception:
            # If config can't be loaded, just use empty string (will fall back to defaults)
            pass
    
    if external_handlers_list:
        # Parse comma-separated list (handle spaces)
        extensions = [ext.strip().lower() for ext in external_handlers_list.split(",") if ext.strip()]
        if extension_name.lower() in extensions:
            # Extension is in the list - use conventions
            # Get region from system Lambda's region
            lambda_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
            
            config = {
                "has_external_handlers": True,
                "active": True,
                "lambda_function_name": f"{extension_name}-handlers",  # Convention
                "lambda_region": lambda_region,  # Same as system Lambda
                "docker_image": f"{extension_name}-lambda-builder:latest",  # Convention
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
        config = {
            "has_external_handlers": env_enabled == "true",
            "active": os.getenv(f"{env_prefix}ACTIVE", "true").lower() == "true",
            "lambda_function_name": os.getenv(f"{env_prefix}LAMBDA_FUNCTION", f"{extension_name}-handlers"),
            "lambda_region": lambda_region,
            "docker_image": os.getenv(f"{env_prefix}DOCKER_IMAGE", f"{extension_name}-lambda-builder:latest"),
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
    
    return {
        "docker_image": config.get("docker_image", f"{extension_name}-lambda-builder:latest"),
        "ecs_docker_image": config.get("ecs_docker_image", f"{extension_name}-ecs-builder:latest"),
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


def get_ecs_handlers(extension_name: str) -> list:
    """
    Return list of handler names that run on ECS for this extension.
    Empty list if none or extension not configured.
    """
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
    Reads from extensions/<name>/installer/service/ecs_deploy_config.json if present (written by deploy),
    then falls back to env ECS_RESULTS_BUCKET, ECS_CLUSTER, ECS_TASK_DEFINITION, ECS_SUBNETS, ECS_SECURITY_GROUPS.
    """
    config = load_extension_config(extension_name)
    if not config or not config.get("has_external_handlers", False):
        return None
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    file_cfg = _load_ecs_deploy_config(extension_name) or {}

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
    subnets = _list("subnets", "ECS_SUBNETS")
    security_groups = _list("security_groups", "ECS_SECURITY_GROUPS")
    # Auto-fill from default VPC if bucket/cluster are set but network is missing
    if bucket and cluster and (not subnets or not security_groups):
        default_net = _get_default_vpc_network_config(region)
        if not subnets and default_net.get("subnets"):
            subnets = default_net["subnets"]
        if not security_groups and default_net.get("security_groups"):
            security_groups = default_net["security_groups"]
    if not bucket or not cluster or not subnets or not security_groups:
        return None
    return {
        "region": region,
        "s3_bucket": bucket,
        "cluster": cluster,
        "task_definition": task_def,
        "subnets": subnets,
        "security_groups": security_groups,
        "payload_prefix": "payloads",
        "result_prefix": "results",
    }
