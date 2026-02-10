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
from typing import Dict, Any, Optional


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
    
    Args:
        extension_name: Name of the extension
        
    Returns:
        Dict with docker_image, package_path (auto-detected), or None
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
        "package_path": package_path
    }
