"""Manage the SCALE-MS API Context.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as Context instances.

This module allows the Python interpreter to track a global stack or tree
structure to allow for simpler syntax and clean resource deallocation.
"""

import asyncio
import concurrent.futures
import importlib
import os
import warnings
import weakref
from concurrent.futures._base import _T, Future
from types import TracebackType
from typing import Any, Callable, Optional, Tuple


class AbstractLocalContext:
    asyncio = None

    def asynchronous(self):
        # TODO: Look for something more canonical, like attributes indicating certain
        # protocols, or event loop interfaces.
        return self.asyncio is asyncio

    def __init__(self):
        subprocess = importlib.import_module('subprocess', self.asyncio)
        self.PIPE = getattr(subprocess, 'PIPE')
        self.STDOUT = getattr(subprocess, 'STDOUT')
        self.DEVNULL = getattr(subprocess, 'DEVNULL')
        self.subprocess = subprocess


class ImmediateExecutionContext(AbstractLocalContext):
    """Workflow context for immediately executed commands.

    Commands are executed immediately upon addition to the work flow. Data flow
    must be resolvable at command instantiation.

    Intended for debugging.
    """
    def subprocess_runner(self, *args, **kwargs):
        p = self.subprocess.Popen(*args, **kwargs)
        p.communicate()

    def run(self, task):
        # If task belongs to this context, it has already run: no-op.
        ...

    def wait(self, awaitable):
        # Warning: this is not the right way to confirm the object does not need await...
        assert not asyncio.iscoroutine(awaitable)
        raise NotImplementedError()


class AsyncExecutionContext(AbstractLocalContext):
    """Standard basic workflow context for local execution.

    Uses the asyncio module to allow commands to be staged as asyncio coroutines.

    There is no implicit OS level multithreading or multiprocessing.
    """
    asyncio = asyncio

    def run(self, task):
        # Call asyncio.run()
        ...

    def wait(self, awaitable, **kwargs):
        # TODO: We have to confirm that an event loop is running and properly handle awaitables.
        assert asyncio.iscoroutine(awaitable)
        raise NotImplementedError()


class LocalExecutor(concurrent.futures.Executor):
    """Perform local execution in terms of the asyncio module."""
    @staticmethod
    async def wrap_function(fn: Callable[..., _T], *args: Any, **kwargs: Any):
        # TODO: resolve Futures in arguments.
        assert callable(fn)
        return fn(*args, **kwargs)

    def submit(self, fn: Callable[..., _T], *args: Any, **kwargs: Any):
        """Overrides concurrent.futures.Executor.

        Create a Task by scheduling *fn* to be executed with the provided arguments.

        Note that this implementation is not thread-safe and needs to be
        reconsidered. Currently it provides experimental internal implementation support.
        """
        assert callable(fn)
        if asyncio.iscoroutinefunction(fn):
            task = fn(*args, **kwargs)
            assert asyncio.iscoroutine(task)
        else:

        return task

    def __init__(self):
        _context.append(self)
        self.__active = True

    def shutdown(self, **kwargs):
        """Extends concurrent.futures.Executor.
        :param **kwargs: passed to base class.
        """
        if self.__active:
            context = _context.pop()
            if context is not self:
                # TODO: Use contextvars to localize state data.
                warnings.warn('Bad shut down protocol may indicate race condition or leak: LocalExecutor is active, but not current.')
                _context.append(context)
            self.__active = False
        else:
            warnings.warn('LocalExecutor.shutdown has been called more than once.')
        return super().shutdown(**kwargs)

    def __del__(self):
        if self.__active:
            warnings.warn('LocalExecutor was not explicitly shut down.')
            self.shutdown()

    def __enter__(self):
        """Extends concurrent.futures.Executor"""
        assert get_context() is self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Extends concurrent.futures.Executor"""
        self.shutdown()
        # Return False to indicate we have not handled any exceptions.
        return False