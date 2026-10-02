"""
Placement for extensions that run on a peer.

An extension is on the hub or on exactly one peer. This module tells the hub
which handles are placed on a peer and how to call that peer's Lambda. Handlers
for a hub-placed extension stay in-process via SchdLoader.

Loading order:
    1. Peer route map (``PEER_ROUTES``, else SSM ``/{wl}/bootstrap/peer-routes``)
    2. ``PEER_EXTENSIONS`` comma-separated handles (laptop / catalog)
    3. ``{HANDLE}_PEER_*`` fields
    4. ``DEFAULT_CONFIG`` in this file

Names from before this rename (``EXTERNAL_HANDLERS*``) are still read so a
running deploy keeps routing until its env is rewritten.
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
        #     "on_peer": True,
        #     "active": True,
        #     "lambda_function_name": "arbitium-handlers",
        #     "lambda_region": "us-east-1",
        #     "docker_image": "arbitium-lambda-builder:latest",
        #     "package_path": "extensions/arbitium/package",  # Optional - auto-detected if not provided
        # }
    }
}


PEER_ROUTES_ENV = "PEER_ROUTES"
PEER_ROUTES_ENV_PREV = "EXTERNAL_HANDLERS_PEER_MAP"
PEER_ROUTING_ENV = "PEER_ROUTING"
PEER_ROUTING_ENV_PREV = "EXTERNAL_HANDLERS_PEER_ROUTING"
PEER_EXTENSIONS_ENV = "PEER_EXTENSIONS"
PEER_EXTENSIONS_ENV_PREV = "EXTERNAL_HANDLERS"
PEER_ROUTES_SSM_ENV = "PEER_ROUTES_SSM"
PEER_ROUTES_SSM_ENV_PREV = "EXTERNAL_HANDLERS_PEER_ROUTES_SSM"
_OFF_VALUES = frozenset({"off", "0", "false", "no"})


def _first_setting(*keys: str) -> str:
    """First non-empty environment or config value. Later keys are previous deploy names."""
    for key in keys:
        raw = (os.getenv(key) or "").strip()
        if raw:
            return raw
    try:
        from renglo.common import load_config

        cfg = load_config()
    except Exception:
        return ""
    if not isinstance(cfg, dict):
        return ""
    for key in keys:
        raw = cfg.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            return text
    return ""


def peer_routing_enabled() -> bool:
    """Kill-switch: ``PEER_ROUTING=off`` ignores the peer map (fail closed)."""
    raw = (
        os.getenv(PEER_ROUTING_ENV) or os.getenv(PEER_ROUTING_ENV_PREV) or "on"
    ).strip().lower()
    if not raw:
        return True
    return raw not in _OFF_VALUES


def _peer_map_from_mapping(data: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(data, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for handle, row in data.items():
        key = str(handle).strip().lower()
        if not key or not isinstance(row, dict):
            continue
        out[key] = dict(row)
    return out


def _peer_map_from_ssm() -> Dict[str, Dict[str, Any]]:
    """Optional runtime SSM so the map can change without a backend BOM deploy."""
    path = (os.getenv(PEER_ROUTES_SSM_ENV) or os.getenv(PEER_ROUTES_SSM_ENV_PREV) or "").strip()
    if not path:
        wl = (os.getenv("WL_NAME") or "").strip()
        if wl:
            path = f"/{wl}/bootstrap/peer-routes"
    if not path:
        return {}
    try:
        import boto3

        region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        raw = boto3.client("ssm", region_name=region).get_parameter(Name=path, WithDecryption=True)
        value = str(raw["Parameter"]["Value"]).strip()
        if not value:
            return {}
        parsed = json.loads(value)
        if isinstance(parsed, dict) and "routes" in parsed and isinstance(parsed["routes"], dict):
            parsed = parsed["routes"]
        return _peer_map_from_mapping(parsed)
    except Exception:
        return {}


def _peer_map_from_raw(raw: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(raw, dict):
        return _peer_map_from_mapping(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return _peer_map_from_mapping(parsed)
    return {}


def load_peer_map() -> Dict[str, Dict[str, Any]]:
    """Handle → peer route. JSON in env, else load_config(), else SSM peer-routes."""
    for key in (PEER_ROUTES_ENV, PEER_ROUTES_ENV_PREV):
        mapped = _peer_map_from_raw(os.getenv(key) or "")
        if mapped:
            return mapped
    try:
        from renglo.common import load_config

        cfg = load_config()
        if isinstance(cfg, dict):
            for key in (PEER_ROUTES_ENV, PEER_ROUTES_ENV_PREV):
                mapped = _peer_map_from_raw(cfg.get(key))
                if mapped:
                    return mapped
    except Exception:
        pass
    return _peer_map_from_ssm()


def get_peer_route(extension_name: str) -> Optional[Dict[str, Any]]:
    """Return the peer route for this handle, or None when unmapped / kill-switch off."""
    if not peer_routing_enabled():
        return None
    handle = (extension_name or "").strip().lower()
    if not handle:
        return None
    return load_peer_map().get(handle)


def _function_name_from_lambda_arn(arn: str) -> str:
    """Extract function name from arn:aws:lambda:...:function:name[:qualifier]."""
    arn = arn.strip()
    if ":function:" not in arn:
        return arn
    tail = arn.split(":function:", 1)[1]
    return tail.split(":")[0] if tail else arn


def _peer_lambda_function_name(route: Dict[str, Any]) -> str:
    fn = str(route.get("lambda_function_name") or "").strip()
    if fn:
        return fn
    arn = str(route.get("lambda_arn") or "").strip()
    if arn:
        return _function_name_from_lambda_arn(arn)
    return ""


def _resolve_handlers_lambda_function_name(extension_name: str) -> str:
    """Peer Lambda name when mapped; otherwise ``{extension_name}-handlers`` (laptop)."""
    route = get_peer_route(extension_name)
    if route:
        fn = _peer_lambda_function_name(route)
        if fn:
            return fn
    return f"{extension_name}-handlers"


def _peer_extension_names() -> list[str]:
    raw = _first_setting(PEER_EXTENSIONS_ENV, PEER_EXTENSIONS_ENV_PREV)
    return [ext.strip().lower() for ext in raw.split(",") if ext.strip()]


def resolve_handlers_docker_image_base(extension_name: str) -> str:
    """Docker image base for this handle (peer image when mapped).

    Priority:
      1. Peer route function name (``{env}-peer-{peerId}-lambda-builder``)
      2. ``{extension_name}-lambda-builder`` (laptop / unmapped)
    """
    route = get_peer_route(extension_name)
    if route:
        fn = _peer_lambda_function_name(route)
        if fn:
            return f"{fn}-lambda-builder"
        image = str(route.get("docker_image_base") or "").strip()
        if image:
            return image
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


def _peer_field(extension_name: str, field: str, default: str = "") -> str:
    """``{HANDLE}_PEER_{FIELD}``, else the previous ``{HANDLE}_EXTERNAL_HANDLERS_{FIELD}``."""
    handle = extension_name.upper().replace("-", "_")
    current = os.getenv(f"{handle}_PEER_{field}")
    if current is not None and str(current).strip() != "":
        return str(current)
    previous = os.getenv(f"{handle}_EXTERNAL_HANDLERS_{field}")
    if previous is not None and str(previous).strip() != "":
        return str(previous)
    return default


def load_extension_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """Return how to call this handle when it is placed on a peer.

    A peer route wins. Otherwise the handle must be listed in ``PEER_EXTENSIONS``
    (or the previous ``EXTERNAL_HANDLERS`` name). ``{HANDLE}_PEER_*`` is the
    per-handle override. Hub-placed handles return None and stay in-process.
    """
    config = None
    route = get_peer_route(extension_name)
    if route:
        lambda_region = (
            str(route.get("region") or "").strip()
            or os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
            or "us-east-1"
        )
        image_base = resolve_handlers_docker_image_base(extension_name)
        return {
            "on_peer": True,
            "active": True,
            "lambda_function_name": _resolve_handlers_lambda_function_name(extension_name),
            "lambda_region": lambda_region,
            "docker_image": f"{image_base}:latest",
            "ecs_docker_image": f"{resolve_handlers_ecs_image_base(extension_name)}:latest",
            "extension_name": extension_name,
        }

    # Laptop / catalog: handles placed on a peer, when no route row is present.
    extensions = _peer_extension_names()
    
    if extensions:
        if extension_name.lower() in extensions:
            lambda_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
            image_base = resolve_handlers_docker_image_base(extension_name)
            
            config = {
                "on_peer": True,
                "active": True,
                "lambda_function_name": _resolve_handlers_lambda_function_name(extension_name),
                "lambda_region": lambda_region,
                "docker_image": f"{image_base}:latest",
                "ecs_docker_image": f"{resolve_handlers_ecs_image_base(extension_name)}:latest",
                "extension_name": extension_name
            }
            return config
    
    env_enabled = _peer_field(extension_name, "ENABLED").lower()
    if env_enabled in ["true", "false"]:
        lambda_region = (
            _peer_field(extension_name, "LAMBDA_REGION")
            or os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
            or "us-east-1"
        )
        image_base = resolve_handlers_docker_image_base(extension_name)
        default_docker = f"{image_base}:latest"
        config = {
            "on_peer": env_enabled == "true",
            "active": _peer_field(extension_name, "ACTIVE", "true").lower() == "true",
            "lambda_function_name": (
                _peer_field(extension_name, "LAMBDA_FUNCTION")
                or _resolve_handlers_lambda_function_name(extension_name)
            ),
            "lambda_region": lambda_region,
            "docker_image": _peer_field(extension_name, "DOCKER_IMAGE", default_docker),
            "ecs_docker_image": f"{resolve_handlers_ecs_image_base(extension_name)}:latest",
            "extension_name": extension_name
        }
        return config
    
    if extension_name in DEFAULT_CONFIG.get("extensions", {}):
        default_config = DEFAULT_CONFIG["extensions"][extension_name].copy()
        if "on_peer" in default_config and isinstance(default_config["on_peer"], dict):
            default_config = default_config["on_peer"]
        elif "external_handlers" in default_config and isinstance(default_config["external_handlers"], dict):
            default_config = default_config["external_handlers"]
        default_config["extension_name"] = extension_name
        return default_config
    
    return None


def placed_on_peer(extension_name: str) -> bool:
    """True when this handle is placed on a peer. Hub-placed handles return False."""
    config = load_extension_config(extension_name)
    if not config:
        return False
    if "on_peer" in config:
        return bool(config.get("on_peer"))
    return bool(config.get("has_external_handlers"))


def peer_is_active(extension_name: str) -> bool:
    """True when the peer placement is configured and not turned off."""
    config = load_extension_config(extension_name)
    if not config:
        return False
    on_peer = config.get("on_peer", config.get("has_external_handlers"))
    return bool(config.get("active", True) and on_peer)


def get_lambda_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Get Lambda configuration for an extension.
    
    Args:
        extension_name: Name of the extension
        
    Returns:
        Dict with lambda_function_name and lambda_region, or None
    """
    config = load_extension_config(extension_name)
    if not placed_on_peer(extension_name):
        return None
    
    return {
        "function_name": config.get("lambda_function_name", f"{extension_name}-handlers"),
        "region": config.get("lambda_region", "us-east-1")
    }


def get_local_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Get local Docker configuration for an extension placed on a peer.
    
    Returns docker_image (Lambda builder) and ecs_docker_image (ECS builder).
    
    Args:
        extension_name: Name of the extension
        
    Returns:
        Dict with docker_image, ecs_docker_image, package_path (auto-detected), or None
    """
    config = load_extension_config(extension_name)
    if not config or not placed_on_peer(extension_name):
        return None
    
    # Resolve package_path without assuming extensions/{handle}/ folder name.
    package_path = config.get("package_path") or resolve_extension_package_path(extension_name)
    
    image_base = resolve_handlers_docker_image_base(extension_name)
    ecs_base = resolve_handlers_ecs_image_base(extension_name)
    return {
        "docker_image": config.get("docker_image", f"{image_base}:latest"),
        "ecs_docker_image": config.get("ecs_docker_image", f"{ecs_base}:latest"),
        "package_path": package_path
    }


def _package_path_env_keys(extension_name: str) -> tuple[str, str]:
    suffix = extension_name.upper().replace("-", "_")
    return (f"PEER_PACKAGE_{suffix}", f"EXTERNAL_HANDLERS_PACKAGE_{suffix}")


def _read_extension_handle_marker(package_dir: Path) -> str:
    """Optional ``extension_handle`` file in package dir decouples handle from folder name."""
    marker = package_dir / "extension_handle"
    if not marker.is_file():
        return ""
    try:
        return marker.read_text(encoding="utf-8").strip().lower()
    except OSError:
        return ""


def resolve_extension_package_dir(extension_name: str) -> Optional[Path]:
    """Locate handler package source for local dev without assuming folder == handle.

    Resolution order:
      1. ``PEER_PACKAGE_{HANDLE}`` env (absolute or workspace-relative)
      2. ``extensions/{handle}/package`` when present
      3. Scan ``extensions/*/package/extension_handle`` for a matching handle
    """
    handle = (extension_name or "").strip().lower()
    if not handle:
        return None
    root = _get_workspace_root()
    if not root:
        return None

    override = ""
    for key in _package_path_env_keys(handle):
        override = (os.getenv(key) or "").strip()
        if override:
            break
    if override:
        candidate = Path(override)
        if not candidate.is_absolute():
            candidate = root / candidate
        if candidate.is_dir():
            return candidate

    direct = root / "extensions" / handle / "package"
    if direct.is_dir():
        return direct

    ext_root = root / "extensions"
    if not ext_root.is_dir():
        return None
    for child in sorted(ext_root.iterdir()):
        if not child.is_dir():
            continue
        package_dir = child / "package"
        if not package_dir.is_dir():
            continue
        if _read_extension_handle_marker(package_dir) == handle:
            return package_dir
    return None


def resolve_extension_package_path(extension_name: str) -> str:
    """Workspace-relative path to handler package dir, or legacy ``extensions/{handle}/package``."""
    found = resolve_extension_package_dir(extension_name)
    root = _get_workspace_root()
    if found and root:
        try:
            return found.relative_to(root).as_posix()
        except ValueError:
            return str(found)
    return f"extensions/{extension_name}/package"


def get_ecs_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Get ECS invocation config for an extension (cluster, task definition, S3 bucket, network).
    Used when invoking handlers via ECS run_task + S3 results.
    Reads ECS_* env vars / deploy_input (no laptop ecs_deploy_config.json).
    """
    config = load_extension_config(extension_name)
    if not config or not placed_on_peer(extension_name):
        return None
    route = get_peer_route(extension_name)
    if not route:
        return None
    cluster = str(route.get("ecs_cluster") or route.get("cluster") or "").strip()
    if not cluster:
        return None
    region = (
        str(route.get("region") or "").strip()
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    file_cfg: Dict[str, Any] = {}
    for src_key, dst_key in (
        ("ecs_cluster", "cluster"),
        ("cluster", "cluster"),
        ("ecs_task_definition", "task_definition"),
        ("task_definition", "task_definition"),
        ("ecs_results_bucket", "s3_bucket"),
        ("s3_bucket", "s3_bucket"),
        ("launch_type", "launch_type"),
        ("network_mode", "network_mode"),
        ("subnets", "subnets"),
        ("security_groups", "security_groups"),
    ):
        val = route.get(src_key)
        if val not in (None, ""):
            file_cfg.setdefault(dst_key, val)

    def _str(key: str, env_key: str, default: str = "") -> str:
        if key in file_cfg and file_cfg.get(key) not in (None, ""):
            return str(file_cfg[key]).strip()
        return default

    def _list(key: str, env_key: str) -> list:
        from_file = file_cfg.get(key)
        if isinstance(from_file, list) and from_file:
            return [str(x).strip() for x in from_file if x]
        return []

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


def get_async_s3_config(extension_name: str) -> Optional[Dict[str, Any]]:
    """
    Get S3 config for async payload/result storage (bucket, region, prefixes).
    Used by async start/result/status when running locally (in-process or dev Docker)
    without full ECS. Prefer get_ecs_config when available; otherwise use global
    ECS_RESULTS_BUCKET + AWS_REGION so any handler can run in async mode.
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


get_batch_s3_config = get_async_s3_config
