"""Manage the SCALE-MS API Context.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as Context instances.

This module allows the Python interpreter to track a global stack or tree
structure to allow for simpler syntax and clean resource deallocation.
"""

import os
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
        """Allow context instance to provide the asyncio.run interface."""
        assert self.__active
        assert get_context() is self

        import asyncio
        from asyncio.coroutines import iscoroutine
        assert iscoroutine(coroutine)

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
        """Allow context instance to provide the asyncio.run interface."""
        assert self.__active
        assert get_context() is self

        import asyncio
        from asyncio.coroutines import iscoroutine
        assert iscoroutine(coroutine)

        return asyncio.run(coroutine)

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
        self.umgr = None

        _context.append(self)
        # TODO: Couple *active* to the status of self.session?
        self.__active = True

    def finalize(self):
        if self.__active:
            context = _context.pop()
            if context is not self:
                warnings.warn('Bad finalizer protocol may indicate race condition or leak: RPDispatcher is active, but not current.')
                _context.append(context)
            self.session.close()
            self.__active = False
        else:
            warnings.warn('finalize has been called more than once.')

    def __del__(self):
        if self.__active:
            warnings.warn('RPDispatcher was not explicitly finalized.')
            self.finalize()

    def __enter__(self):
        assert get_context() is self
        assert self.session is None
        self.session = self.rp.Session()
        pmgr = self.rp.PilotManager(session=self.session)
        self.umgr = self.rp.UnitManager(session=self.session)
        pilot = pmgr.submit_pilots(self.rp.ComputePilotDescription(self.pilot_description))
        self.umgr.add_pilots(pilot)
        # Note: We should have an active session now, ready to receive tasks, but
        # no tasks have been submitted.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # self.umgr.wait_units()
        self.finalize()
        # Return False to indicate we have not handled any exceptions.
        return False


_context = [DefaultContext()]


def get_context():
    return _context[-1]
