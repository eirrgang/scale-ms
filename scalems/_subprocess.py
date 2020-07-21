"""Commands executed as subprocesses.
"""
import collections
import json
import os
from typing import _T_co, Any, Generator

import scalems.context as scalems_context


async def _rp_exec(task_input: 'radical.pilot.ComputeUnitDescription' = None):
    context = scalems_context.get_context()
    assert isinstance(context, scalems_context.RPDispatcher)
    assert context.umgr is not None
    task_description = {'executable': task_input['argv'][0],
                        'cpu_processes': 1}
    task = context.umgr.submit_units(context.rp.ComputeUnitDescription(task_description))
    # task.wait() just hangs. Using umgr.wait_units() instead...
    # task.wait()
    context.umgr.wait_units()
    assert task.exit_code == 0


def _local_exec(task_description: dict):
    argv = task_description['argv']
    assert isinstance(argv, (list, tuple))
    assert len(argv) > 0
    import subprocess
    return subprocess.run(argv)


def make_cu(context, task_input):
    """InputResource factory for implementations based on standard RP ComputeUnit.

    .. todo:: How can we defer run time argument realization? Is this what the RP "kernel" idea is for?
    """
    from scalems.context.radical import RPContextManager
    if not isinstance(context, RPContextManager):
        raise ValueError('This resource factory is only valid for RADICAL Pilot workflow contexts.')
    task_description = {'executable': task_input['argv'][0],
                        'cpu_processes': 1}
    return context.rp.ComputeUnitDescription(task_description)


def make_subprocess_args(context, task_input):
    """InputResource factory for *subprocess* based implementations.
    """
    # subprocess.Popen and asyncio.create_subprocess_exec have approximately compatible arguments.
    from scalems.context.local import AbstractLocalContext
    if not isinstance(context, AbstractLocalContext):
        raise ValueError('This resource factory is for subprocess-based execution per scalems.context.local')
    # TODO: await the arguments.
    args = list([arg for arg in task_input['argv']])

    # TODO: stream based input with PIPE.
    kwargs = {
        'stdin': None,
        'stdout': None,
        'stderr': None,
        'env': None
    }
    return {'args': args, 'kwargs': kwargs}


def local_runner(session, arguments):
    """Create and execute a subprocess task in the context."""
    # Get callable.
    process = session.wait(session.subprocess_runner(*arguments['args'], **arguments['kwargs']))
    runner = process.wait
    # Call callable.
    # TODO: We should yield in here, somehow, to allow cancellation of the subprocess.
    # Suggest splitting runner into separate launch/resolve phases or representing this
    # long-running function as a stateful object. Note: this is properly a run-time Task.
    # TODO: Split current Context implementations into two components: run time and dispatcher?
    # Or is that just the Context / Session division?
    # Run time needs to allow for task management (asynchronous where applicable) and can
    # be confined to an environment with a running event loop (where applicable) and/or
    # active executor.
    # Dispatcher would be able to insulate callers / collaborators from event loop details.
    session.wait(runner())

    result = SubprocessResult()
    result._exitcode = process.returncode
    assert result.exitcode() is not None
    return result


class SubprocessResult:
    """Hypothetical class for Task result, if we want a separate class for the post-execution state."""
    def __init__(self):
        self._exitcode = None
        self._stdout = None
        self._stderr = None
        self._file = None

    # Design note: We will want to consolidate the expression of outputs to allow
    # automated generation of Futures for coroutine attribute access. Data Descriptor
    # objects for class fields seem like a likely underlying mechanism that could
    # be reused for both Coroutine and Result classes. But we may choose to generate
    # the result type, itself, from the coroutine definition, if we can make sensible
    # static type checking work.
    def exitcode(self):
        return self._exitcode

    def stdout(self):
        return self._stdout

    def stderr(self):
        return self._stdout

    def file(self):
        return self._file


class SubprocessCoroutine(collections.abc.Awaitable):
    """ScaleMS Coroutine object for scalems.executable tasks."""
    def __init__(self, *, context, description: dict = None):
        self.serialized_description = json.dumps(description)
        self._result = None  # type: SubprocessResult

    #
    # def run_in_context(self, context):
    #     # TODO: dispatching
    #     from scalems.context.local import LocalExecutor
    #     if isinstance(context, LocalExecutor):
    #         # Note that we need a more sophisticated coroutine object than what we get directly from `async def`
    #         # for command instances that can present output in multiple contexts or be transferred from one to another.
    #         task_input = json.loads(self.serialized_description)
    #         arguments = make_subprocess_args(context, task_input=task_input)
    #         if hasattr(context, 'subprocess_runner'):
    #             return context.subprocess_runner(*arguments['args'], **arguments['kwargs'])
    #     elif isinstance(context, scalems_context.RPDispatcher):
    #         return await _rp_exec(task_description)
    #     raise NotImplementedError('Current context {} does not implement scalems.executable'.format(context))

    def __await__(self) -> Generator[Any, None, SubprocessResult]:
        """Implements the asyncio protocol for a coroutine object.

        When awaited, query the current context to negotiate dispatching. Note that the
        built-in asyncio module acts like a LocalExecutor Context if and only if there
        is not an active SCALE-MS Context. SCALE-MS Contexts
        set the current context before awaiting.
        """
        context = self.context
        # Local staging not implemented. Immediately dispatch to RP Context.
        if context is None:
            context = scalems_context.get_context()
        # TODO: dispatching
        if isinstance(context, scalems_context.LocalExecutor):
            # Note that we need a more sophisticated coroutine object than what we get directly from `async def`
            # for command instances that can present output in multiple contexts or be transferred from one to another.
            self._result = await _local_exec(task_description)
        elif isinstance(context, scalems_context.RPDispatcher):
            self._result = await _rp_exec(task_description)
        else:
            raise NotImplementedError('Current context {} does not implement scalems.executable'.format(context))

        # Allow this function to be a generator function, fulfilling the awaitable protocol.
        yield self
        # Note that the items yielded are not particularly useful, but the position of the
        # yield expression(s) is potentially useful for debugging or multitasking. Depending
        # on the implementation of the event loop, multiple yields may allow a way to avoid
        # deadlocks. For instance, we may choose to yield at each iteration of a loop to
        # provide or read PIPE-based stdin or stdout. `await` should accomplish the same thing,
        # but the generator protocol may improve debugging and generality.
        # The point of "yield" is more interesting when we use "yield" as an expression in the
        # yielding code, which allows values to be passed in to the coroutine at the evaluation
        # of the yield expression (e.g. https://docs.python.org/3/howto/functional.html#passing-values-into-a-generator
        # but not that the coroutine protocol is slightly different, per https://www.python.org/dev/peps/pep-0492/)
        # For instance, this could be a mechanism for nesting event loops or dispatching contexts
        # while maintaining a heart-beat or other command-channel-like wrapper.

        if not isinstance(self._result, SubprocessResult):
            raise RuntimeError('Result was not delivered!')
        return self._result


def executable(*args, **kwargs):
    """Execute a command line program.

    Note:
        Most users will prefer to use the commandline_operation() helper instead
        of this low-level function.

    Configure an executable to run in one (or more) subprocess(es).
    Executes when run in an execution Context, as part of a work graph.
    Process environment and execution mechanism depends on the execution environment,
    but is likely similar to (or implemented in terms of) the POSIX execvp system call.

    Shell processing of *argv* is disabled to improve determinism.
    This means that shell expansions such as environment variables, globbing (``*``),
    and other special symbols (like ``~`` for home directory) are not available.
    This allows a simpler and more robust implementation, as well as a better
    ability to uniquely identify the effects of a command line operation. If you
    think this disallows important use cases, please let us know.

    Required Arguments:
         argv: a tuple (or list) to be the subprocess arguments, including the executable

    Optional Arguments:
         outputs: labeled output files
         inputs: labeled input files
         environment: environment variables to be set in the process environment
         stdin: source for posix style standard input file handle (default None)
         stdout: Capture standard out to a filesystem artifact, even if it is not consumed in the workflow.
         stderr: Capture standard error to a filesystem artifact, even if it is not consumed in the workflow.
         resources: Name additional required resources, such as an MPI environment.

    .. todo:: Support POSIX sigaction / IPC traps?

    .. todo:: Consider dataclasses.dataclass types to replace reusable/composable function signatures.

    Program arguments are iteratively added to the command line with standard Python
    iteration, so you should use a tuple or list even if you have only one parameter.
    I.e. If you provide a string with ``arguments="asdf"`` then it will be passed as
    ``... "a" "s" "d" "f"``. To pass a single string argument, ``arguments=("asdf")``
    or ``arguments=["asdf"]``.

    *inputs* and *outputs* should be a dictionary with string keys, where the keys
    name command line "flags" or options.

    Note that the Execution Context (e.g. RPContext, LocalContext, DockerContext)
    determines the handling of *resources*. Typical values in *resources* may include
    * procs_per_task (int): Number of processes to spawn for an instance of the *exec*.
    * threads_per_proc (int): Number of threads to allocate for each process.
    * gpus_per_task (int): Number of GPU devices to allocate for and instance of the *exec*.
    * launcher (str): Task launch mechanism, such as `mpiexec`.

    Returns:
        Output collection contains *exitcode*, *stdout*, *stderr*, *file*.

    The *file* output has the same keys as the *outputs* key word argument.

    Example:
        Execute a command named ``exe`` that takes a flagged option for file name
        (stored in a local Python variable ``my_filename``) and an ``origin`` flag
        that uses the next three arguments to define a vector. It is known to the
        user that the command always produces a file called ``exe.out``.

            >>> my_filename = "somefilename"
            >>> result = scalems.executable(('exe', '--origin', 1.0, 2.0, 3.0, '-f', my_filename), inputs={'infile': my_filename}, outputs={'outfile': 'exe.out'})
            >>> assert hasattr(result, 'file')
            >>> assert os.path.exists(result.file['outfile'].result())
            >>> assert hasattr(result, 'exitcode')

    """
    task_description = dict()
    # TODO: Normalized argument handling.
    if len(args) > 0:
        if 'argv' in kwargs or len(args) > 1:
            raise ValueError('Unknown positional arguments provided.')
        task_description['argv'] = args
    task_description.update(kwargs)
    if not 'argv' in task_description:
        raise ValueError('Missing required argument *argv*.')

    # TODO: typing helpers
    if isinstance(task_description['argv'], (str, bytes)):
        raise ValueError('argv should be a proper sequence type for the elements of an argv array.')

    for key in ('outputs', 'inputs', 'environment', 'stdin', 'stdout', 'stderr', 'resources'):
        if key in task_description:
            raise ValueError('Unsupported key word argument: {}'.format(key))

    # TODO: Implement TaskBuilder director.
    awaitable = SubprocessCoroutine(task_description)

    return awaitable
