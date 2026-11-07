"""Azure submodel of LlmModel.

Construction succeeds so the API can start. Model calls themselves are not implemented.
"""


class LlmModel:
    def __init__(self, config=None, *_args, **_kwargs):
        self.config = config if isinstance(config, dict) else {}

    def __getattr__(self, name):
        raise NotImplementedError("LlmModel has no Azure submodel yet")
