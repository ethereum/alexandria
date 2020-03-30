class AlexandriaError(Exception):
    pass


class DecryptionError(AlexandriaError):
    pass


class HandshakeFailure(AlexandriaError):
    pass
