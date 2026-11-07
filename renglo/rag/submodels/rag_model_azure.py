"""Azure submodel of RagModel.

Construction succeeds so the API can start. Retrieval itself is not implemented.
"""


class RagModel:
    def __init__(self, config=None, *_args, **_kwargs):
        self.config = config if isinstance(config, dict) else {}
        self.client = None

    def __getattr__(self, name):
        raise NotImplementedError("RagModel has no Azure submodel yet")
