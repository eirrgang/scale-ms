"""Commands executed as subprocesses.
"""

import os

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


async def _local_exec(task_description: dict):
    argv = task_description['argv']
    assert isinstance(argv, (list, tuple))
    assert len(argv) > 0
    import subprocess
    return subprocess.run(argv)


async def _exec(task_description: dict):
    """Produce the awaitable coroutine for scalems.executable.

    When awaited, query the current context to negotiate dispatching. Note that the
    built-in asyncio module acts like a LocalExecutor Context if and only if there
    is not an active SCALE-MS Context. SCALE-MS Contexts
    set the current context before awaiting.
    """
    # Local staging not implemented. Immediately dispatch to RP Context.
    context = scalems_context.get_context()
    # TODO: dispatching
    if isinstance(context, scalems_context.LocalExecutor):
        # Note that we need a more sophisticated coroutine object than what we get directly from `async def`
        # for command instances that can present output in multiple contexts or be transferred from one to another.
        return await _local_exec(task_description)
    elif isinstance(context, scalems_context.RPDispatcher):
        return await _rp_exec(task_description)
    raise NotImplementedError('Current context {} does not implement scalems.executable'.format(context))


def executable(*args, **kwargs):
    """Execute a command line program.

    Note:
        Most users will prefer to use the commandline_operation() helper instead
        of this low-level function.

    Configure an executable to run in one (or more) subprocess(es).
    Executes when run in an execution Context, as part of a work graph.

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
    if len(args) > 0:
        if 'argv' in kwargs or len(args) > 1:
            raise ValueError('Unknown positional arguments provided.')
        task_description['argv'] = args[0]
    task_description.update(kwargs)
    if not 'argv' in task_description:
        raise ValueError('Missing required argument *argv*.')
    if isinstance(task_description['argv'], (str, bytes)):
        raise ValueError('argv should be a proper sequence type for the elements of an argv array.')
    # TODO: Implement TaskBuilder director.
    return _exec(task_description)
