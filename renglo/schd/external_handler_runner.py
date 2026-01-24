"""
Universal functions for running external handlers (local Docker or Lambda)

These functions are extension-agnostic and work with any extension that has
external handlers configured. Extension-specific information comes from
the external_handlers_config module.
"""

import json
import subprocess
import os
import tempfile
import sys
import shlex
from typing import Dict, Any, Optional
from pathlib import Path

try:
    import boto3
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from renglo.schd.external_handlers_config import (
    get_lambda_config,
    get_local_config,
    is_external_handler_active
)
from renglo.common import load_config


def is_running_locally() -> bool:
    """
    Determine if we're running in a local environment vs Lambda.
    
    Returns:
        True if running locally, False if in Lambda
    """
    # Check for Lambda environment variable
    if os.getenv('AWS_LAMBDA_FUNCTION_NAME'):
        return False
    
    # Default to local if we can't determine
    return True


def load_config_for_docker() -> Dict[str, Any]:
    """
    Load configuration using the stable load_config() function from renglo.common.
    
    This is used to pass config to Docker containers and Lambda functions.
    Uses the same stable function that handlers use, ensuring consistency.
    
    Returns:
        Dictionary of config values
    """
    try:
        return load_config()
    except RuntimeError:
        # If load_config() fails (e.g., critical vars missing), return empty dict
        # The calling code can handle the missing config appropriately
        return {}


def call_local_docker_handler(
    extension_name: str,
    handler_name: str,
    payload: Dict[str, Any]
    ) -> Dict[str, Any]:
    """
    Run a handler locally using Docker.
    
    This is loosely based on run_handler_local.sh but is extension-agnostic.
    It uses the extension's configuration to determine Docker image and paths.
    
    Args:
        extension_name: Name of the extension
        handler_name: Name of the handler to run
        payload: Payload to pass to the handler
        
    Returns:
        Response dict with 'success', 'output', etc.
    """
    config = get_local_config(extension_name)
    if not config:
        return {
            'success': False,
            'error': f'No local configuration found for extension: {extension_name}'
        }
    
    # Auto-detect workspace root
    workspace_root = None
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "extensions").exists() and (parent / "dev").exists():
            workspace_root = str(parent)
            break
    
    if not workspace_root:
        return {
            'success': False,
            'error': 'Could not determine workspace root'
        }
    
    docker_image = config['docker_image']
    package_path = config['package_path']
    full_package_path = os.path.join(workspace_root, package_path)
    
    # Check if Docker is available
    try:
        subprocess.run(['docker', '--version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return {
            'success': False,
            'error': 'Docker is not available or not in PATH'
        }
    
    # Check if Docker image exists
    try:
        result = subprocess.run(
            ['docker', 'image', 'inspect', docker_image],
            capture_output=True,
            check=True
        )
    except subprocess.CalledProcessError:
        return {
            'success': False,
            'error': f'Docker image {docker_image} not found. Please build it first.'
        }
    
    # Create event JSON
    event = {
        'handler': handler_name,
        'payload': payload
    }
    event_json = json.dumps(event)
    
    # Prepare AWS credentials
    aws_dir = os.path.expanduser('~/.aws')
    docker_args = [
        'docker', 'run', '--rm',
        '--entrypoint', '/bin/sh',
        '-v', f'{full_package_path}:/package',
        '-w', '/package'
    ]
    
    # Mount AWS credentials if available
    if os.path.isdir(aws_dir):
        docker_args.extend(['-v', f'{aws_dir}:/root/.aws:ro'])
    
    # Pass AWS environment variables
    aws_env_vars = [
        'AWS_PROFILE', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
        'AWS_SESSION_TOKEN', 'AWS_DEFAULT_REGION', 'AWS_REGION'
    ]
    for env_var in aws_env_vars:
        value = os.getenv(env_var)
        if value:
            docker_args.extend(['-e', f'{env_var}={value}'])
    
    # Pass configuration environment variables (same as run_handler_local.sh does)
    # Load config from system/env_config.py or environment variables
    config = load_config_for_docker()
    for key, value in config.items():
        if value is not None and value != '':
            # Escape the value properly for shell
            docker_args.extend(['-e', f'{key}={shlex.quote(str(value))}'])
    
    # Add image and command
    #
    # NOTE: We avoid f-strings with nested triple quotes here to keep the Python
    # source valid. We build the heredoc script as a normal string and inject
    # the JSON payload via .format().
    python_script = """python3.12 <<'PYTHON_SCRIPT'
import sys
import json
from datetime import datetime, date
from decimal import Decimal

# Normalize function to handle non-JSON-serializable objects
def normalize_for_json(obj):
    # Recursively normalize objects to be JSON-serializable.
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {{k: normalize_for_json(v) for k, v in obj.items()}}
    if isinstance(obj, (list, tuple)):
        return [normalize_for_json(item) for item in obj]
    if hasattr(obj, '__dict__'):
        return normalize_for_json(obj.__dict__)
    # Fallback: convert to string
    return str(obj)

# Custom JSON encoder
class UniversalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return super(UniversalEncoder, self).default(obj)

# Logging setup - send to stderr so it's visible
def log(message):
    print(message, file=sys.stderr)

log("=" * 60)
log("Starting handler execution in Docker")
log("=" * 60)

sys.path.insert(0, '/package')
from lambda_router import lambda_handler

event_str = {event_json!r}
log(f"Event received: {{event_str[:200]}}...")  # Log first 200 chars
event = json.loads(event_str)

try:
    log("Calling lambda_handler...")
    result = lambda_handler(event, None)
    log("Handler execution completed successfully")
    
    # Normalize the result before JSON serialization
    normalized_result = normalize_for_json(result)
    log("Result normalized, serializing to JSON...")
    
    # Use custom encoder for extra safety
    json_output = json.dumps(normalized_result, indent=2, cls=UniversalEncoder)
    log("JSON serialization successful")
    
    # Output JSON to stdout (this is what gets parsed)
    print(json_output)
    
except Exception as e:
    import traceback
    log(f"ERROR: Handler execution failed: {{str(e)}}")
    log(f"Traceback: {{traceback.format_exc()}}")
    
    error_result = {{
        'statusCode': 500,
        'success': False,
        'error': str(e),
        'traceback': traceback.format_exc()
    }}
    normalized_error = normalize_for_json(error_result)
    print(json.dumps(normalized_error, indent=2, cls=UniversalEncoder))
    sys.exit(1)
PYTHON_SCRIPT"""

    docker_args.extend([
        docker_image,
        '-c',
        python_script.format(event_json=event_json),
    ])
    
    # Run Docker container
    try:
        result = subprocess.run(
            docker_args,
            capture_output=True,
            text=True,
            check=True,
            cwd=workspace_root
        )
        
        # Log stderr output for debugging (this contains our log messages)
        if result.stderr:
            print("=== Docker Execution Logs ===", file=sys.stderr)
            print(result.stderr, file=sys.stderr)
            print("=== End Docker Logs ===", file=sys.stderr)
        
        # Parse output
        # The handler may output debug messages before the JSON
        # The JSON is typically the last complete JSON object in the output
        stdout_text = result.stdout.strip()
        json_output = None
        
        # Find the JSON by looking for a '{' that's likely the start of JSON
        # (not a Python dict in debug output like {'key': 'value'})
        # Strategy: Find all '{' positions and check which ones look like JSON
        brace_positions = []
        for i, char in enumerate(stdout_text):
            if char == '{':
                # Check if this looks like JSON:
                # 1. Followed by newline and then whitespace and a quote (pretty-printed JSON)
                # 2. Followed by whitespace and a quote (compact JSON)
                # 3. At the start of a line (after newline)
                looks_like_json = False
                if i + 1 < len(stdout_text):
                    # Check for pretty-printed JSON: {\n  "
                    if i > 0 and stdout_text[i-1] == '\n':
                        # This '{' is at the start of a line - likely JSON
                        looks_like_json = True
                    elif i + 2 < len(stdout_text):
                        next_part = stdout_text[i+1:i+20].lstrip()
                        # Check if followed by quote (JSON key) or newline then quote
                        if next_part.startswith('"') or (next_part.startswith('\n') and '"' in next_part[:30]):
                            looks_like_json = True
                
                if looks_like_json:
                    brace_positions.append(i)
        
        # If no JSON-like braces found, fall back to finding the last '{' (JSON is usually at the end)
        if not brace_positions:
            last_brace = stdout_text.rfind('{')
            if last_brace >= 0:
                brace_positions = [last_brace]
        
        # Try parsing from each potential JSON start position (try first/outermost one first)
        # The outermost JSON object is usually what we want
        for first_brace in brace_positions:
            if first_brace >= 0:
                # Strategy 1: Try to parse from this '{' to the end
                json_text = stdout_text[first_brace:].strip()
                try:
                    json_output = json.loads(json_text)
                    break  # Success! Stop trying other positions
                except json.JSONDecodeError:
                    # Strategy 2: Find the complete JSON object by matching braces
                    brace_count = 0
                    json_start = first_brace
                    json_end = len(stdout_text)
                    
                    for i in range(first_brace, len(stdout_text)):
                        if stdout_text[i] == '{':
                            brace_count += 1
                        elif stdout_text[i] == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                json_end = i + 1
                                break
                    
                    if brace_count == 0:
                        json_text = stdout_text[json_start:json_end].strip()
                        try:
                            json_output = json.loads(json_text)
                            break  # Success! Stop trying other positions
                        except json.JSONDecodeError:
                            continue  # Try next brace position
        
        if json_output:
            # Convert Lambda handler response format to SchdLoader format
            if json_output.get('statusCode') == 200 and json_output.get('success'):
                return {
                    'success': True,
                    'output': json_output.get('body', {})
                }
            else:
                return {
                    'success': False,
                    'output': json_output.get('error') or json_output.get('body', {}),
                    'error': json_output.get('error', 'Handler execution failed')
                }
        else:
            # Log the raw output for debugging
            print("=== Could not parse JSON from Docker output ===", file=sys.stderr)
            print("STDOUT:", file=sys.stderr)
            print(result.stdout, file=sys.stderr)
            if result.stderr:
                print("STDERR:", file=sys.stderr)
                print(result.stderr, file=sys.stderr)
            print("=== End Docker output ===", file=sys.stderr)
            
            return {
                'success': False,
                'error': 'Could not parse handler output as JSON. Check logs for details.',
                'raw_output': result.stdout,
                'raw_stderr': result.stderr
            }
            
    except subprocess.CalledProcessError as e:
        # Log stderr for debugging
        if e.stderr:
            print("=== Docker Execution Error Logs ===", file=sys.stderr)
            print(e.stderr, file=sys.stderr)
            print("=== End Docker Error Logs ===", file=sys.stderr)
        if e.stdout:
            print("=== Docker Execution Output ===", file=sys.stderr)
            print(e.stdout, file=sys.stderr)
            print("=== End Docker Output ===", file=sys.stderr)
        
        return {
            'success': False,
            'error': f'Docker execution failed: {e.stderr}',
            'raw_output': e.stdout
        }


def call_lambda_handler(
    extension_name: str,
    handler_name: str,
    payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Call a handler via AWS Lambda.
    
    This is loosely based on test_lambda_handler.py but is extension-agnostic.
    It uses the extension's configuration to determine Lambda function name and region.
    
    Args:
        extension_name: Name of the extension
        handler_name: Name of the handler to invoke
        payload: Payload to pass to the handler
        
    Returns:
        Response dict with 'success', 'output', etc.
    """
    if not BOTO3_AVAILABLE:
        return {
            'success': False,
            'error': 'boto3 is not available. Install it to use Lambda handlers.'
        }
    
    config = get_lambda_config(extension_name)
    if not config:
        return {
            'success': False,
            'error': f'No Lambda configuration found for extension: {extension_name}'
        }
    
    function_name = config['function_name']
    region = config['region']
    
    # Create Lambda client
    try:
        lambda_client = boto3.client('lambda', region_name=region)
    except Exception as e:
        return {
            'success': False,
            'error': f'Failed to create Lambda client: {str(e)}'
        }
    
    # Prepare the event
    event = {
        'handler': handler_name,
        'payload': payload
    }
    
    # Note: Lambda environment variables are set at the function level (not per invocation)
    # They should be configured when the Lambda is deployed (via Zappa's environment_variables
    # in zappa_settings.json). We check if critical vars are missing and update if needed.
    # Load config to check/update Lambda function environment variables
    config = load_config_for_docker()
    
    # Check if critical config variables are missing from Lambda function
    # and update them if needed (only updates once, not on every invocation)
    try:
        func_config = lambda_client.get_function_configuration(FunctionName=function_name)
        current_env = func_config.get('Environment', {}).get('Variables', {})
        
        # Check if critical config variables are missing
        critical_vars = ['DYNAMODB_RINGDATA_TABLE', 'DYNAMODB_ENTITY_TABLE']
        missing_critical = [k for k in critical_vars 
                           if k not in current_env and k in config and config[k]]
        
        # Check for any other config vars that should be in Lambda but aren't
        vars_to_update = {k: str(v) for k, v in config.items() 
                         if k not in current_env and v is not None and v != ''}
        
        if missing_critical:
            # Update Lambda function environment variables with missing critical vars
            # Merge with existing env vars to avoid overwriting
            updated_env = {**current_env, **vars_to_update}
            lambda_client.update_function_configuration(
                FunctionName=function_name,
                Environment={'Variables': updated_env}
            )
            # Wait a moment for the update to propagate
            import time
            time.sleep(1)
            print(f"Updated Lambda function environment variables: {list(vars_to_update.keys())}", file=sys.stderr)
        elif vars_to_update:
            # Non-critical vars are missing, but we'll let the function use defaults
            # or rely on what's configured at deployment time
            pass
    except Exception as e:
        # If we can't check/update env vars, log a warning but continue
        # The Lambda function should already have env vars configured via deployment
        print(f"Warning: Could not check/update Lambda environment variables: {e}", file=sys.stderr)
        print("Note: Ensure Lambda function has environment variables configured via deployment (zappa_settings.json)", file=sys.stderr)
    
    try:
        # Invoke the function
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',  # Synchronous
            Payload=json.dumps(event)
        )
        
        # Read the response
        response_payload = json.loads(response['Payload'].read())
        
        # Check for function errors
        if 'FunctionError' in response:
            return {
                'success': False,
                'error': f'Lambda function error: {response["FunctionError"]}',
                'output': response_payload
            }
        
        # Convert Lambda handler response format to SchdLoader format
        if response_payload.get('statusCode') == 200 and response_payload.get('success'):
            return {
                'success': True,
                'output': response_payload.get('body', {})
            }
        else:
            return {
                'success': False,
                'output': response_payload.get('error') or response_payload.get('body', {}),
                'error': response_payload.get('error', 'Handler execution failed')
            }
            
    except Exception as e:
        return {
            'success': False,
            'error': f'Failed to invoke Lambda function: {str(e)}'
        }


def run_external_handler(
    extension_name: str,
    handler_name: str,
    payload: Dict[str, Any]
    ) -> Dict[str, Any]:
    """
    Run an external handler (automatically chooses local Docker or Lambda).
    
    This is the main entry point that abstracts away the choice between
    local and Lambda execution. It automatically detects the environment
    and calls the appropriate function.
    
    Args:
        extension_name: Name of the extension
        handler_name: Name of the handler to run
        payload: Payload to pass to the handler
        
    Returns:
        Response dict with 'success', 'output', etc.
    """
    # Check if external handlers are active
    if not is_external_handler_active(extension_name):
        return {
            'success': False,
            'error': f'External handlers for {extension_name} are not active or not configured'
        }
    
    # Determine execution mode
    if is_running_locally():
        return call_local_docker_handler(extension_name, handler_name, payload)
    else:
        return call_lambda_handler(extension_name, handler_name, payload)
