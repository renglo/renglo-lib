import json

import boto3
import requests

DEFAULT_EBE_URL = 'http://127.0.0.1:5056'


class SchdModel:

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        self.client = None
        self._destination_arn = None

    def _use_ebe(self, backend=None):
        """Local origin talks to ebe; cloud origin talks to EventBridge. They coexist."""
        return str(backend or 'cloud').strip().lower() == 'local'

    def _emulator_url(self):
        return str(self.config.get('EVENTBRIDGE_EMULATOR_URL') or DEFAULT_EBE_URL).rstrip('/')

    def _events_client(self):
        if self.client is None:
            aws_region = self.config.get('AWS_REGION', 'us-east-1')
            self.client = boto3.client('events', region_name=aws_region)
        return self.client

    def _local_request(self, method, path, body=None, params=None):
        url = f'{self._emulator_url()}{path}'
        try:
            resp = requests.request(method, url, json=body, params=params, timeout=15)
        except Exception as e:
            return {'success': False, 'output': str(e)}
        if resp.status_code >= 400:
            return {'success': False, 'output': resp.text}
        try:
            data = resp.json()
        except Exception:
            data = {'status': resp.status_code}
        if isinstance(data, dict):
            if 'success' not in data:
                data = {'success': True, 'output': data}
            return data
        return {'success': True, 'output': data}

    def ingress_destination_name(self):
        named = str(self.config.get('RENGLO_INGRESS_DESTINATION') or '').strip()
        if named:
            return named
        env_name = str(self.config.get('WL_NAME') or '').strip()
        if env_name:
            return f'{env_name}-renglo-process'
        return ''

    def ingress_destination_arn(self):
        """Look up the shared webhook API Destination by name."""
        if self._destination_arn:
            return self._destination_arn
        name = self.ingress_destination_name()
        if not name:
            return ''
        try:
            response = self._events_client().describe_api_destination(Name=name)
            arn = str(response.get('ApiDestinationArn') or '').strip()
            if arn:
                self._destination_arn = arn
            return arn
        except Exception:
            return ''

    def create_https_target_event(self, rule_name, schedule_expression, payload, backend='cloud'):
        """Create a scheduled rule that POSTs to /_schd/ingress via the webhook API Destination."""
        job_payload = dict(payload or {})
        if not job_payload.get('type'):
            job_payload['type'] = 'schd_job'

        if self._use_ebe(backend):
            return self._local_request(
                'PUT',
                '/rules',
                {
                    'name': rule_name,
                    'kind': 'schedule',
                    'schedule_expression': schedule_expression,
                    'payload': job_payload,
                    'machine_id': job_payload.get('schd_machine_id') or '',
                    'enabled': True,
                },
            )

        print('action:create_https_target_events')
        try:
            response_1 = self._events_client().put_rule(
                Name=rule_name,
                ScheduleExpression=schedule_expression,
                State='ENABLED',
            )
            if 'RuleArn' not in response_1:
                return {'success': False, 'output': response_1}
            print(response_1)
        except Exception as e:
            return {'success': False, 'output': str(e)}

        result = self._put_ingress_target(rule_name, job_payload)
        if not result.get('success'):
            try:
                self._events_client().delete_rule(Name=rule_name)
            except Exception:
                pass
            return result

        return {
            'success': True,
            'message': 'Rule created successfully',
            'input': {
                'rule_name': rule_name,
                'schedule_expression': schedule_expression,
                'payload': payload,
            },
            'output': response_1,
        }

    def delete_https_target_event(self, rule_name, backend='cloud'):
        """Delete an EventBridge rule and its associated target."""
        if self._use_ebe(backend):
            return self._local_request('DELETE', f'/rules/{rule_name}')
        try:
            client = self._events_client()
            response_1 = client.remove_targets(
                Rule=rule_name,
                Ids=[rule_name + '_target'],
                Force=True,
            )
            if response_1['FailedEntryCount'] > 0:
                return {'success': False, 'message': 'Failed to remove target', 'output': response_1}

            response_2 = client.delete_rule(Name=rule_name)
            return {
                'success': True,
                'message': 'Rule and target deleted successfully',
                'input': {'rule_name': rule_name},
                'output': {'remove_targets': response_1, 'delete_rule': response_2},
            }
        except Exception as e:
            print(e)
            return {'success': False, 'message': 'Failed to delete rule', 'output': str(e)}

    def find_rule(self, rulename):
        action = 'find_rule'
        paginator = self._events_client().get_paginator('list_rules')
        for page in paginator.paginate():
            for rule in page['Rules']:
                print(f'Rule >>> {rule}')
                if rule.get('Name') == rulename:
                    return {'success': True, 'action': action, 'input': rulename, 'output': rule}
        return {'success': False, 'input': rulename, 'output': False}

    def set_rule_state(self, rule_name, enabled=True, backend='cloud'):
        """Enable or disable an EventBridge rule without deleting it."""
        if self._use_ebe(backend):
            return self._local_request('PATCH', f'/rules/{rule_name}', {'enabled': enabled})
        try:
            client = self._events_client()
            client.enable_rule(Name=rule_name) if enabled else client.disable_rule(Name=rule_name)
            return {'success': True, 'rule_name': rule_name, 'enabled': enabled}
        except Exception as e:
            return {'success': False, 'output': str(e)}

    def create_event_pattern_target(self, rule_name, event_source, payload_defaults=None, backend='cloud'):
        """Rule that matches PutEvents source and POSTs to /_schd/ingress."""
        if self._use_ebe(backend):
            return {'success': True, 'message': 'Local fan-out does not use a pattern rule'}

        pattern = json.dumps({"source": [event_source]})
        try:
            response_1 = self._events_client().put_rule(
                Name=rule_name,
                EventPattern=pattern,
                State='ENABLED',
            )
            if 'RuleArn' not in response_1:
                return {'success': False, 'output': response_1}
        except Exception as e:
            return {'success': False, 'output': str(e)}

        job_payload = dict(payload_defaults or {})
        result = self._put_ingress_target(rule_name, job_payload)
        if not result.get('success'):
            try:
                self._events_client().delete_rule(Name=rule_name)
            except Exception:
                pass
            return result
        return {
            'success': True,
            'message': 'Event pattern rule created',
            'output': response_1,
        }

    def put_schd_events(self, entries, backend=None):
        """PutEvents onto the default bus. Each entry is a detail dict."""
        if not entries:
            return {'success': True, 'output': {'FailedEntryCount': 0}}
        if backend is None:
            first = entries[0] if entries else {}
            backend = 'local' if str((first or {}).get('schedule_origin') or '') == 'local' else 'cloud'
        if self._use_ebe(backend):
            result = self._local_request('POST', '/events', {'entries': list(entries)})
            if result.get('success'):
                result.setdefault('output', {'FailedEntryCount': 0, 'queued': result.get('queued')})
            return result
        try:
            response = self._events_client().put_events(
                Entries=[
                    {
                        'Source': 'custom.renglo.schd',
                        'DetailType': 'SchdJob',
                        'Detail': json.dumps(detail),
                    }
                    for detail in entries
                ]
            )
            failed = int(response.get('FailedEntryCount') or 0)
            return {'success': failed == 0, 'output': response}
        except Exception as e:
            return {'success': False, 'output': str(e)}

    def _put_ingress_target(self, rule_name, job_payload):
        """Attach the shared webhook API Destination. Auth is on the Connection."""
        target_arn = self.ingress_destination_arn()
        if not target_arn:
            name = self.ingress_destination_name() or 'RENGLO_INGRESS_DESTINATION'
            return {
                'success': False,
                'output': f'API Destination not found ({name}). Deploy the webhook ingress stack.',
            }
        if 'api-destination' not in target_arn:
            return {
                'success': False,
                'output': f'Ingress target must be an API Destination ARN, got {target_arn}',
            }
        target = {
            'Id': rule_name + '_target',
            'Arn': target_arn,
            'RoleArn': self.config.get('ROLE_ARN', ''),
            'HttpParameters': {
                'HeaderParameters': {'Content-Type': 'application/json'},
                'PathParameterValues': [],
                'QueryStringParameters': {},
            },
        }
        if job_payload:
            target['Input'] = json.dumps(job_payload)
        try:
            response_2 = self._events_client().put_targets(Rule=rule_name, Targets=[target])
            if response_2.get('FailedEntryCount'):
                return {'success': False, 'output': response_2}
            return {'success': True, 'output': response_2, 'target_arn': target_arn}
        except Exception as e:
            return {'success': False, 'output': str(e)}

    def append_local_activity(self, entry):
        return self._local_request('POST', '/activity', dict(entry or {}))

    def list_local_activity(self, **filters):
        params = {key: value for key, value in (filters or {}).items() if value not in (None, '')}
        return self._local_request('GET', '/activity', params=params)

    def get_local_activity(self, event_id):
        return self._local_request('GET', f'/activity/{event_id}')
