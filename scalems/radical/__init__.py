"""Entry point subpackage for ScaleMS execution dispatching to RADICAL Pilot.

Example:
    python -m scalems.radical myworkflow.py

Manage workflow context for RADICAL Pilot.

Dispatching through RADICAL Pilot is still evolving, and this
module may provide multiple disparate concepts.

Context Manager:
    RPContextManager provides a SCALE-MS workflow context and coordinates
    resources for a RADICAL Pilot Session. When "entered" (i.e. used as
    a :py:func:`with`), the Python Context Manager protocol manages the
    lifetime of a radical.pilot.Session. Two significant areas of future
    development include Context chaining, and improved support for multiple rp.Sessions
    through multiple RPContextManager instances.

Executor:
    RPExecutor is an attempt to extend concurrent.futures.Executor.
    Unlike concurrent.futures.Executor, RPExecutor is not designed
    to accept arbitrary Python functions through *submit*. Also,
    the executor is not intended to be instantiated directly by users,
    but should be obtained through a RPContext instance in a scoped
    block. This allows better lifetime management of the executor
    and of the associated rp.Session.

Event loop:
    The concurrent.futures.Executor multi-tasking model may not be a useful
    abstraction for RADICAL Pilot. Alternatives include interacting
    more directly with the core asyncio functionality. To support such
    a model, "thread" contextual details for RPContextManager are stored
    as contextvars.ContextVar module variables, as appropriate.
    Module functions allow RP tasks to be expressed as Python awaitables,
    which use the asynchronous context manager protocol to manage
    rp.Session lifecycle in a coroutine function that is used to produce
    an awaitable workflow object.

"""
# TODO: Consider converting to a namespace package to improve modularity of implementation.


import asyncio
import concurrent.futures
import os
import warnings
import weakref
from concurrent.futures import Future
from types import TracebackType
from typing import Any, Callable, Optional, Tuple

import scalems.context


class RPContextManager:
    def __init__(self):
        import radical.pilot as rp
        self.rp = rp


class RPResult:
    """Basic result type for RADICAL Pilot tasks.

    Define a return type for Futures or awaitable tasks from
    RADICAL Pilot commands.
    """


class RPFuture(concurrent.futures.Future):
    """Future interface for RADICAL Pilot tasks."""

    def __init__(self, task) -> None:
        super().__init__()
        if not callable(task):
            raise ValueError('Provide a callable that produces the rp ComputeUnit.')
        self.task = task

    def cancel(self) -> bool:
        raise NotImplementedError()

    def cancelled(self) -> bool:
        return super().cancelled()

    def running(self) -> bool:
        raise NotImplementedError()

    def add_done_callback(self, fn: Callable[[Future], Any]) -> None:
        # TODO: more complete type hinting.
        raise NotImplementedError()

    def result(self, timeout: Optional[float] = ...) -> RPResult:
        if not self.done():
            # Note that task.wait() seems not to work reliably.
            # TODO: task.umgr.wait_units(uids=taskid)
            # Warning: Waiting on all units will deadlock in non-trivial cases.
            task = self.task()
            task.umgr.wait_units(uids=task.uid, timeout=timeout)
        return super().result()

    def set_running_or_notify_cancel(self) -> bool:
        raise NotImplementedError()

    def exception(self, timeout: Optional[float] = ...) -> Optional[BaseException]:
        raise NotImplementedError()

    def set_exception(self, exception: Optional[BaseException]) -> None:
        super().set_exception(exception)

    def exception_info(self, timeout: Optional[float] = ...) -> Tuple[Any, Optional[TracebackType]]:
        return super().exception_info(timeout)

    def set_exception_info(self, exception: Any, traceback: Optional[TracebackType]) -> None:
        super().set_exception_info(exception, traceback)


class RPExecutor(concurrent.futures.Executor):
    def __init__(self):
        import radical.pilot as rp
        self.rp = rp
        self.__rp_cfg = dict()
        if not 'RADICAL_PILOT_DBURL' in os.environ:
            raise RuntimeError('RADICAL Pilot environment is not available.')

        resource = 'local.localhost'
        # TODO: Find default config?
        resource_config = {resource: {}}
        resource_config[resource].update({
            'project': None,
            'queue': None,
            'schema': None,
            'cores': 1,
            'gpus': 0
        })
        pilot_description = dict(resource=resource,
                                 runtime=30,
                                 exit_on_error=True,
                                 project=resource_config[resource]['project'],
                                 queue=resource_config[resource]['queue'],
                                 cores=resource_config[resource]['cores'],
                                 gpus=resource_config[resource]['gpus'])
        self.resource_config = resource_config
        self.pilot_description = pilot_description
        self.session = None
        self._finalizer = None
        self.umgr = None

        # TODO: Integrate with event loop scoping to make sure the following occur in a contextvars.Context.
        parent = scalems.context.current.get()
        scalems.context.parent.set(parent)
        scalems.context.current.set(self)
        # TODO: Couple *active* to the status of self.session?
        self.__active = True

    def active(self) -> bool:
        if self.session is None:
            return False
        else:
            return not self.session.closed()

    def submit(self, task_description: dict) -> Future:
        # TODO: more complete type hinting.
        # TODO: The `submit` signature should only apply to Python functions.
        task = self.umgr.submit_units(
            self.rp.ComputeUnitDescription(task_description))  # radical.pilot.ComputeUnit
        # task.wait() just hangs. Using umgr.wait_units() instead...
        # task.wait()
        task_ref = weakref.ref(task)
        future = RPFuture(task_ref)
        def cb(obj, state):
            # Where is the state enumeration?
            # TODO: assert state in [...]
            if task_ref().exit_code is not None:
                future.set_result(RPResult())
        task.register_callback(cb)
        return future

    def shutdown(self):
        if self.active():
            context = scalems.context.current.get()
            # TODO: Use contextvars to localize state data.
            if context is not self:
                warnings.warn('Bad shutdown protocol may indicate race condition or leak: RPDispatcher is active, but not current.')
            else:
                # TODO: Maintain context hierarchy...
                scalems.context.current.set(scalems.context.parent.get())
            self.session.close()
            assert self.session.closed()
        else:
            warnings.warn('shutdown has been called more than once.')

    def __del__(self):
        if self.active():
            warnings.warn('RPDispatcher was not explicitly shutdown.')

    def __enter__(self):
        assert scalems.context.get_context() is self
        assert self.session is None
        session = self.rp.Session()
        self._finalizer = weakref.finalize(self, session.close)
        self.session = session
        pmgr = self.rp.PilotManager(session=self.session)
        self.umgr = self.rp.UnitManager(session=self.session)
        pilot = pmgr.submit_pilots(self.rp.ComputePilotDescription(self.pilot_description))
        self.umgr.add_pilots(pilot)
        # Note: We should have an active session now, ready to receive tasks, but
        # no tasks have been submitted.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # self.umgr.wait_units()
        self.shutdown()
        # Return False to indicate we have not handled any exceptions.
        return False
