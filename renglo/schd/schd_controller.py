from renglo.logger import get_logger

from renglo.data.data_controller import DataController
from renglo.files.files_controller import FilesController
from renglo.blueprint.blueprint_controller import BlueprintController
from renglo.auth.auth_controller import AuthController
from renglo.auth.authorize import authorize

from renglo.schd.schd_loader import SchdLoader
from renglo.schd.schd_model import SchdModel
from renglo.schd.schd_schedule import SchdScheduleMixin
from renglo.schd.external_handlers_config import has_external_handlers, is_external_handler_active, get_ecs_config, get_async_s3_config
from renglo.schd.external_handler_runner import (
    run_external_handler,
    call_heavy_handler_async,
    call_local_docker_handler_async_start,
    get_async_result as run_get_async_result,
    get_async_status as run_get_async_status,
    use_dev_docker,
    write_async_payload,
    write_async_result,
)
from renglo.runtime import attach_auth_roles_to_payload, attach_jwt_claims_to_payload

from datetime import datetime

import json
import os
import threading
import uuid

class SchdController(SchdScheduleMixin):

    def __init__(self, config=None):
        self.config = config or {}
        self.logger = get_logger()
        self.DAC = DataController(config=self.config)
        self.FCC = FilesController(config=self.config)
        self.BPC = BlueprintController(config=self.config)
        self.AUC = AuthController(config=self.config)
        self.SHM = SchdModel(config=self.config)
        self.SHL = SchdLoader()

    @staticmethod
    def _pop_reserved_flag(payload, key):
        if not isinstance(payload, dict):
            return False
        value = payload.pop(key, False)
        if value is True or value == 1:
            return True
        if isinstance(value, str) and value.strip().lower() in ("1", "true", "yes"):
            return True
        return False

    @staticmethod
    def _maybe_stack(result, stack, include_stack):
        if include_stack:
            result["stack"] = stack
        return result

    def find_rule(self,portfolio,org,timer):
        
        rule_name = "cron_"+portfolio+"_"+org+"_"+timer        
        result = self.SHM.find_rule(rule_name)
        
        return result
        
    
    @authorize()
    def create_rule(self,portfolio,org,name,schedule_expression,payload):
        '''
        Function used to create the cronjob
        '''
        rule_name = "cron_"+portfolio+"_"+org+"_"+name

        result = self.SHM.create_https_target_event(
            rule_name=rule_name,
            schedule_expression=schedule_expression,
            payload=payload
        )

        
        return result
    

    @authorize()
    def remove_rule(self,portfolio,org,name):
        '''
        Function used to create the cronjob
        '''
        rule_name = "cron_"+portfolio+"_"+org+"_"+name
        
        result = self.SHM.delete_https_target_event(rule_name)
        
        return result
        
        
    def verify_rule(self,portfolio,org,timer):
        
        rule_name = "cron_"+portfolio+"_"+org+"_"+timer        
        result = self.SHM.find_rule(rule_name)
        
        return result
    
   
    def create_job_run(self,portfolio,org,payload):
        '''Execute a scheduled or manual job. Logs to schd_activity, not schd_runs.'''
        self.logger.debug('Action: create_job_run:')
        return self.execute_job(portfolio, org, payload)
        
        
    
    def direct_run(self,handler,payload):
           
        result = []

        action = 'direct_run'
        
        print(f'Calling handler:{handler}, payload:{payload}')
             
        response = {'success':False,'output':[]}
        
        # A way to limit the calls to this endpoint is to make each one of these runs have the same name as a blueprint. 
        # And before every run, we could fetch the blueprint. It if doesn't exist we abort the call. 
        # It makes sense that there is a blueprint for every RPC as it shows the inputs of the call. 
        # We could store every call to the RPC as a document. The ring itself is the name of the blueprint. 
        parts = handler.split('/')
        if len(parts)==2:
            extension = parts[0]
            handler_name = parts[1]
        else:
            result.append({'success':False,'action':action,'input':payload,'output':response})
            return result, 400

        payload['tool'] = extension

        if has_external_handlers(extension) and is_external_handler_active(extension):
            print(f'Calling external handler:{handler}')
            attach_jwt_claims_to_payload(payload)
            response = run_external_handler(
                extension_name=extension,
                handler_name=handler_name,
                payload=payload
            )
            if not response.get('success'):
                result.append({'success': False, 'action': action, 'handler': handler_name, 'input': payload, 'output': response})
                return result, 400
            result.append({'success': True, 'action': action, 'handler': handler_name, 'input': payload, 'output': response})
            return result, 200

        '''
        # This check exists because there should be a blueprint that defines the input shape of the handler. 
        # This only applies to handlers that are exposed publicly. 
        # We are commenting it as this is no longer a hard requirement. 
        if extension != '_action':
            blueprint = self.BPC.get_blueprint('irma',handler_name,'last')
            print('Blueprint:',blueprint)
        
            if 'fields' not in blueprint:
                print(blueprint)
                result.append({'success':False,'action':action,'input':payload,'error':f'Error with the blueprint:{handler_name}'}) 
                return result, 400
        '''
           
        response = self.SHL.load_and_run(handler, payload = payload)
        
        #print(f'Handler output:{response}')
        
        
        if not response['success']:
            result.append({'success':False,'action':action,'handler':handler_name,'input':payload,'output':response})
            return result, 400
        
        result.append({'success':True,'action':action,'handler':handler_name,'input':payload,'output':response})

        return result, 200
    
    
    
    def _resolve_extension_handle(self, portfolio, extension):
        """
        Resolve a URL extension segment to the tool handle used by handlers.

        - If already a valid handle (e.g. "productora"), return it as-is.
        - If it is a tool id (e.g. "9def1b8eaa83"), resolve to tool.handle.
        """
        value = str(extension or "").strip()
        if not value:
            return value

        # Already a valid python package name/handle.
        if value.isidentifier() and not value[0].isdigit():
            return value

        try:
            found = self.AUC.get_installable(portfolio, value)
            if found:
                document = found.get('document') or {}
                handle = str(document.get('handle') or '').strip()
                if handle:
                    self.logger.debug(f"Resolved tool id '{value}' to handle '{handle}'")
                    return handle
        except Exception as e:
            self.logger.warning(f"Could not resolve tool id '{value}' to handle: {e}")

        return value

    @authorize(resource="tool", tool_id_param="extension")
    def handler_call(self,portfolio,org,extension,handler,payload):
        action = 'handler_call'
        print(f'Calling handler:{handler}, payload:{payload}')
        
        try:
            payload = payload or {}
            include_stack = self._pop_reserved_flag(payload, '_stack')
            resolved_extension = self._resolve_extension_handle(portfolio, extension)

            # We override portfolio, org and extension that might come in the payload.
            payload['portfolio'] = portfolio
            payload['org'] = org
            payload['tool'] = resolved_extension 
                
            response = {'success':False,'output':[]}
            
            # Switch logic: Check if extension has external handlers
            if has_external_handlers(resolved_extension):
                # Extension has external handlers configured
                if is_external_handler_active(resolved_extension):
                    # External handlers are active - use external handler runner
                    # This automatically chooses local Docker or Lambda based on environment
                    attach_jwt_claims_to_payload(payload)
                    response = run_external_handler(
                        extension_name=resolved_extension,
                        handler_name=handler,
                        payload=payload
                    )
                    
                    # Convert external handler response format to match SchdLoader format
                    # SchdLoader returns: {'success': bool, 'output': {'output': [...], 'interface': ...}}
                    # External handlers return: {'success': bool, 'output': {...}}
                    if not response.get('success'):
                        # External handler failed - format to match SchdLoader error format
                        error_output = response.get('output', {})
                        error_msg = response.get('error', 'External handler execution failed [SCOH]')
                        
                        # Create error output in SchdLoader format
                        formatted_output = {
                            'output': error_output if isinstance(error_output, list) else [error_output],
                            'error': error_msg
                        }
                        
                        return self._maybe_stack({
                            'success': False,
                            'action': action,
                            'handler': handler,
                            'input': payload,
                            'output': formatted_output.get('output', [error_msg]),
                        }, response, include_stack)
                    else:
                        # External handler succeeded - convert to SchdLoader format
                        external_output = response.get('output', {})
                        
                        # Wrap in SchdLoader format: {'output': {...}, 'interface': ...}
                        formatted_output = {
                            'output': external_output
                        }
                        
                        # Extract interface if present
                        if isinstance(external_output, dict) and 'interface' in external_output:
                            formatted_output['interface'] = external_output.get('interface')
                        
                        # Extract canonical output (the actual result)
                        if isinstance(external_output, dict):
                            canonical = external_output.get('output', external_output)
                            interface = formatted_output.get('interface')
                        else:
                            canonical = external_output
                            interface = None
                        
                        return self._maybe_stack({
                            'success': True,
                            'action': action,
                            'handler': handler,
                            'input': payload,
                            'interface': interface,
                            'output': canonical,
                        }, {'success': True, 'output': formatted_output}, include_stack)
                else:
                    # External handlers are deactivated - fall back to internal
                    print(f'External handlers for {resolved_extension} are deactivated, using internal handler')
                    response = self.SHL.load_and_run(f'{resolved_extension}/{handler}', payload=payload)
            else:
                # Extension does not have external handlers - use internal handler loader
                response = self.SHL.load_and_run(f'{resolved_extension}/{handler}', payload=payload)

            # Handle internal handler response (SchdLoader format).
            # When load_and_run fails (e.g. exception), response['output'] can be a string, not a dict.
            out = response.get('output')
            if not isinstance(out, dict):
                canonical = [out] if out is not None else [response.get('error', 'Handler failed')]
                return self._maybe_stack({
                    'success': False,
                    'action': action,
                    'handler': handler,
                    'input': payload,
                    'output': canonical,
                }, response, include_stack)
            if not response.get('success'):
                canonical = out.get('output', out)
                if not isinstance(canonical, list):
                    canonical = [canonical] if canonical is not None else []
                return self._maybe_stack({
                    'success': False,
                    'action': action,
                    'handler': handler,
                    'input': payload,
                    'output': canonical,
                }, response, include_stack)
            canonical = out.get('output', out)
            interface = out.get('interface') if isinstance(out, dict) else None
            return self._maybe_stack({
                'success': True,
                'action': action,
                'handler': handler,
                'input': payload,
                'interface': interface,
                'output': canonical,
            }, response, include_stack)

        except Exception as e:
            print(f'Error @handler_call:: {e}')
            return {'success':False,'action':action,'handler':handler,'input':payload,'output':f'Error @handler_call:: {e}'}
        
        

    @authorize(resource="tool", tool_id_param="extension")
    def handler_check(self,portfolio,org,extension,handler,payload):
        action = 'handler_check'
        print(f'Calling handler check:{handler}, payload:{payload}')
        
        try:
            payload = payload or {}
            include_stack = self._pop_reserved_flag(payload, '_stack')
            resolved_extension = self._resolve_extension_handle(portfolio, extension)

            # We override portfolio, org and extension that might come in the payload.
            payload['portfolio'] = portfolio
            payload['org'] = org
            payload['tool'] = resolved_extension
                
            response = {'success':False,'output':[]}
            
            response = self.SHL.load_and_run(f'{resolved_extension}/{handler}', payload = payload, check=True)

            out = response.get('output')
            if not isinstance(out, dict):
                canonical = out if out is not None else response.get('error', 'Handler check failed')
                return self._maybe_stack({
                    'success': False,
                    'action': action,
                    'handler': handler,
                    'input': payload,
                    'output': canonical,
                }, response, include_stack)
            if not response.get('success'):
                canonical = out.get('output', out)
                return self._maybe_stack({
                    'success': False,
                    'action': action,
                    'handler': handler,
                    'input': payload,
                    'output': canonical,
                }, response, include_stack)
            canonical = out.get('output', out)
            interface = out.get('interface') if isinstance(out, dict) else None
            return self._maybe_stack({
                'success': True,
                'action': action,
                'handler': handler,
                'input': payload,
                'interface': interface,
                'output': canonical,
            }, response, include_stack)

        except Exception as e:
            print(f'Error @handler_check: {e}')
            return {'success':False,'action':action,'handler':handler,'input':payload,'output':f'Error @handler_call: {e}'}

    def _run_async_local_worker(self, extension: str, handler: str, payload: dict, request_id: str) -> None:
        """Background worker: run handler in-process and write result to S3."""
        try:
            response = self.SHL.load_and_run(f'{extension}/{handler}', payload=payload)
            write_async_result(extension, request_id, response)
        except Exception as e:
            write_async_result(extension, request_id, {'success': False, 'output': str(e), 'error': str(e)})

    @authorize(resource="tool", tool_id_param="extension")
    def handler_call_async_start(self, portfolio, org, extension, handler, payload):
        """Start an async handler job (any handler). Local in-process, dev Docker, or heavy ECS."""
        action = 'handler_call_async_start'
        payload = dict(payload or {})
        # Re-stamp roles after copying payload (decorator stamped the original dict).
        auth_ctx = getattr(self, "_auth_context", None) or {}
        attach_auth_roles_to_payload(payload, auth_ctx.get("roles") or [])
        resolved_extension = self._resolve_extension_handle(portfolio, extension)
        payload['portfolio'] = portfolio
        payload['org'] = org
        payload['tool'] = resolved_extension

        s3_cfg = get_ecs_config(resolved_extension) if has_external_handlers(resolved_extension) else None
        if not s3_cfg:
            s3_cfg = get_async_s3_config(resolved_extension)
        if not s3_cfg:
            return {'success': False, 'error': 'Async requires ECS_RESULTS_BUCKET or ECS config for result storage'}

        if has_external_handlers(resolved_extension) and is_external_handler_active(resolved_extension):
            attach_jwt_claims_to_payload(payload)
            if use_dev_docker(resolved_extension):
                response = call_local_docker_handler_async_start(
                    extension_name=resolved_extension,
                    handler_name=handler,
                    payload=payload,
                )
            else:
                # Hub does not classify handlers. /start means the peer ECS path.
                response = call_heavy_handler_async(
                    extension_name=resolved_extension,
                    handler_name=handler,
                    payload=payload,
                )
            if not response.get('success'):
                return {
                    'success': False,
                    'action': action,
                    'error': response.get('error', 'Async start failed'),
                    'request_id': None,
                    'task_id': None,
                }
            return {
                'success': True,
                'action': action,
                'request_id': response.get('request_id'),
                'task_id': response.get('task_id'),
            }

        # Local (no external): run handler in background thread and write result to S3
        request_id = str(uuid.uuid4())
        event = {'handler': handler, 'payload': payload}
        try:
            write_async_payload(resolved_extension, request_id, event)
        except Exception as e:
            return {'success': False, 'error': f'Failed to write async payload: {e}'}
        thread = threading.Thread(
            target=self._run_async_local_worker,
            args=(resolved_extension, handler, payload, request_id),
            daemon=True,
        )
        thread.start()
        return {
            'success': True,
            'action': action,
            'request_id': request_id,
            'task_id': None,
        }

    handler_call_batch_start = handler_call_async_start

    @authorize()
    def get_async_result(self, portfolio, org, extension, request_id):
        """Return async result from S3 or pending."""
        result = run_get_async_result(extension_name=extension, request_id=request_id)
        result['portfolio'] = portfolio
        result['org'] = org
        result['extension'] = extension
        return result

    get_batch_result = get_async_result

    @authorize()
    def get_async_status(self, portfolio, org, extension, request_id):
        """Return async progress from S3 (status/<request_id>.json)."""
        result = run_get_async_status(extension_name=extension, request_id=request_id)
        result['portfolio'] = portfolio
        result['org'] = org
        result['extension'] = extension
        return result

    get_batch_status = get_async_status

    def delete_rule(self, rule_name):
        try:
            # List rules before deletion
            rules_before = eventbridge.list_rules(NamePrefix=rule_name)
            logger.info(f"Rules before deletion: {rules_before}")
            
            # Delete the rule
            response = eventbridge.delete_rule(Name=rule_name)
            
            # List rules after deletion to confirm
            rules_after = eventbridge.list_rules(NamePrefix=rule_name)
            logger.info(f"Rules after deletion: {rules_after}")
            
            return response
        except Exception as e:
            logger.error(f"Error deleting rule: {str(e)}")
            raise
