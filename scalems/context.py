"""Manage the SCALE-MS API Context.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as Context instances.

This module allows the Python interpreter to track a global stack or tree
structure to allow for simpler syntax and clean resource deallocation.
"""

import warnings


class AbstractContext:
    """Abstract base class for SCALE-MS workflow Contexts.

    TODO: Enforce centralization of Context instantiation for the interpreter process.
    For instance:
    * Implement a root context singleton and require acquisition of new Context
      handles through methods in this module.
    * Use abstract base class machinery to register Context implementations.
    * Require Context instances to track their parent Context, or otherwise
      participate in a single tree structure.
    * Prevent instantiation of Command references without a reference to a Context instance.
    """
    def run(self, *args, **kwargs):
        raise NotImplementedError('Dispatching not yet implemented.')


class DefaultContext(AbstractContext):
    """Manage workflow data and metadata, but defer execution to sub-contexts.

    Not yet implemented or used.
    """


class LocalExecutor(AbstractContext):
    """Perform immediate local execution."""
    def run(self, coroutine):
        import asyncio
        from asyncio.coroutines import iscoroutine
        assert iscoroutine(coroutine)
        assert get_context() is self
        return asyncio.run(coroutine)

    def __init__(self):
        _context.append(self)
        self.__active = True

    def finalize(self):
        if self.__active:
            context = _context.pop()
            if context is not self:
                warnings.warn('Bad finalizer protocol may indicate race condition or leak: LocalExecutor is active, but not current.')
                _context.append(context)
            self.__active = False
        else:
            warnings.warn('LocalExecutor.finalize has been called more than once.')

    def __del__(self):
        if self.__active:
            warnings.warn('LocalExecutor was not explicitly finalized.')
            self.finalize()

    def __enter__(self):
        assert get_context() is self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalize()
        # Return False to indicate we have not handled any exceptions.
        return False


class RPDispatcher(AbstractContext):
    """Dispatch tasks through RADICAL Pilot."""
    def run(self, coroutine):
        import asyncio
        from asyncio.coroutines import iscoroutine
        assert iscoroutine(coroutine)
        assert get_context() is self
        return asyncio.run(coroutine)

    def __init__(self):
        _context.append(self)
        self.__active = True

    def finalize(self):
        if self.__active:
            context = _context.pop()
            if context is not self:
                warnings.warn('Bad finalizer protocol may indicate race condition or leak: RPDispatcher is active, but not current.')
                _context.append(context)
            self.__active = False
        else:
            warnings.warn('finalize has been called more than once.')

    def __del__(self):
        if self.__active:
            warnings.warn('RPDispatcher was not explicitly finalized.')
            self.finalize()

    def __enter__(self):
        assert get_context() is self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalize()
        # Return False to indicate we have not handled any exceptions.
        return False


_context = [DefaultContext()]


def get_context():
    return _context[-1]
