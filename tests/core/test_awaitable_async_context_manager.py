import pytest
import trio


from alexandria.client import AwaitableAsyncContextManager


@pytest.mark.trio
async def test_awaitable_asynccontextmanager_basic_await():
    did_run = trio.Event()

    async def get_value():
        did_run.set()
        return 7

    obj = AwaitableAsyncContextManager(get_value())
    assert not did_run.is_set()
    result = await obj
    assert result == 7


@pytest.mark.trio
async def test_awaitable_asynccontextmanager_context_manager():
    did_run = trio.Event()

    async def get_value():
        did_run.set()
        return 7

    async with AwaitableAsyncContextManager(get_value()):
        assert not did_run.is_set()
    assert did_run.is_set()


@pytest.mark.trio
async def test_awaitable_asynccontextmanager_context_manager_manually_resolved():
    did_run = trio.Event()

    async def get_value():
        did_run.set()
        return 7

    async with AwaitableAsyncContextManager(get_value()) as obj:
        assert not did_run.is_set()
        result = await obj
        assert did_run.is_set()
        assert result == 7
