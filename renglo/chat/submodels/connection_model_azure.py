"""Azure submodel of ConnectionModel.

Construction succeeds so the API can start. Socket delivery itself is not implemented.
"""


class ConnectionModel:
    def __init__(self, *_args, **_kwargs):
        return None

    def __getattr__(self, name):
        raise NotImplementedError("ConnectionModel has no Azure submodel yet")
