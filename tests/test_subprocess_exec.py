"""Test the scalems._subprocess.exec command."""

import asyncio

import pytest
import scalems.context as sms_context
from scalems._subprocess import executable


def test_exec():
    # Test default context
    cmd = executable('/bin/echo')
    # Check for expected behavior of the default context
    with pytest.raises(NotImplementedError):
        context = sms_context.get_context()
        context.run(cmd)
    # Clean up un-awaited coroutine object, noting default behavior with no specified context.
    with pytest.raises(NotImplementedError):
        asyncio.run(cmd)

    # Test LocalExecutor
    # Note that a coroutine object created from an `async def` function is only awaitable once.
    cmd = executable('/bin/echo')
    context = sms_context.LocalExecutor()
    with context as session:
        session.run(cmd)

    # Test RPDispatcher context
    # Note that a coroutine object created from an `async def` function is only awaitable once.
    cmd = executable('/bin/echo')
    context = sms_context.RPDispatcher()
    with context as session:
        with pytest.raises(NotImplementedError):
            session.run(cmd)