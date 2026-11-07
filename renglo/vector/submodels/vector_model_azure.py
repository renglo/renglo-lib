"""Azure submodel of VectorModel.

Construction succeeds so the API can start. Vector search itself is not implemented.
"""


class VectorModel:
    def __init__(self, config=None, *_args, **_kwargs):
        self.config = config if isinstance(config, dict) else {}

    def __getattr__(self, name):
        raise NotImplementedError("VectorModel has no Azure submodel yet")
