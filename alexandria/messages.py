from typing import NamedTuple


class Ping(NamedTuple):
    id: int


class Pong(NamedTuple):
    ping_id: int
