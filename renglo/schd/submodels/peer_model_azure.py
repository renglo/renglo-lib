"""Azure submodel of PeerModel.

Construction succeeds so the API can start. Peer compute itself is not implemented.
"""


class PeerModel:
    def __init__(self, *_args, **_kwargs):
        return None

    def __getattr__(self, name):
        raise NotImplementedError("PeerModel has no Azure submodel yet")
