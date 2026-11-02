"""SchdModel interface.

Callers construct this class. CLOUD in config selects
renglo.schd.submodels.schd_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.schd.submodels.schd_model_aws import (
    DEFAULT_EBE_URL,
)

def _submodel(config):
    return load_submodel("renglo.schd.submodels", "schd_model", "SchdModel", config)


class SchdModel:
    def __init__(self, config=None, tid=False, ip=False):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, tid, ip))

    def _use_ebe(self, backend=None):
        """Local origin talks to ebe; cloud origin talks to EventBridge. They coexist."""
        return self._impl._use_ebe(backend)

    def _emulator_url(self):
        return self._impl._emulator_url()

    def _events_client(self):
        return self._impl._events_client()

    def _local_request(self, method, path, body=None, params=None):
        return self._impl._local_request(method, path, body, params)

    def ingress_destination_name(self):
        return self._impl.ingress_destination_name()

    def ingress_destination_arn(self):
        """Look up the shared webhook API Destination by name."""
        return self._impl.ingress_destination_arn()

    def create_https_target_event(self, rule_name, schedule_expression, payload, backend='cloud'):
        """Create a scheduled rule that POSTs to /_schd/ingress via the webhook API Destination."""
        return self._impl.create_https_target_event(rule_name, schedule_expression, payload, backend)

    def delete_https_target_event(self, rule_name, backend='cloud'):
        """Delete an EventBridge rule and its associated target."""
        return self._impl.delete_https_target_event(rule_name, backend)

    def find_rule(self, rulename):
        return self._impl.find_rule(rulename)

    def set_rule_state(self, rule_name, enabled=True, backend='cloud'):
        """Enable or disable an EventBridge rule without deleting it."""
        return self._impl.set_rule_state(rule_name, enabled, backend)

    def create_event_pattern_target(self, rule_name, event_source, payload_defaults=None, backend='cloud'):
        """Rule that matches PutEvents source and POSTs to /_schd/ingress."""
        return self._impl.create_event_pattern_target(rule_name, event_source, payload_defaults, backend)

    def put_schd_events(self, entries, backend=None):
        """PutEvents onto the default bus. Each entry is a detail dict."""
        return self._impl.put_schd_events(entries, backend)

    def _put_ingress_target(self, rule_name, job_payload):
        """Attach the shared webhook API Destination. Auth is on the Connection."""
        return self._impl._put_ingress_target(rule_name, job_payload)

    def append_local_activity(self, entry):
        return self._impl.append_local_activity(entry)

    def list_local_activity(self, **filters):
        return self._impl.list_local_activity(**filters)

    def get_local_activity(self, event_id):
        return self._impl.get_local_activity(event_id)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
