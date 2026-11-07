"""Azure submodel of SchdModel.

Construction succeeds so the API can start. Scheduling itself is not implemented.
"""


class SchdModel:
    def __init__(self, config=None, *_args, **_kwargs):
        self.config = config if isinstance(config, dict) else {}

    def __getattr__(self, name):
        raise NotImplementedError("SchdModel has no Azure submodel yet")
