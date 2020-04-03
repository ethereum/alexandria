import collections
from typing import Mapping

from async_service import Service

from alexandria.abc import ClientAPI


class ContentManager(Service):
    def __init__(self,
                 content_db: Mapping[bytes, bytes],
                 client: ClientAPI,
                 ) -> None:
        self.content_db = content_db
        self.content_index = collections.defaultdict(set)
        self.client = client

    async def run(self) -> None:
        ...

    async def _periodically_report(self) -> None:
        ...

    async def _periodically_announce_content(self) -> None:
        ...

    async def _handle_have_requests(self) -> None:
        ...

    async def _handle_get_requests(self) -> None:
        ...

    async def _handle_locate_requests(self) -> None:
        ...
