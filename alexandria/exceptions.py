class AlexandriaError(Exception):
    pass


class DecryptionError(AlexandriaError):
    pass


class HandshakeFailure(AlexandriaError):
    pass


class SessionNotFound(AlexandriaError):
    pass


class DuplicateSession(AlexandriaError):
    pass


class ContentNotFound(AlexandriaError):
    pass
