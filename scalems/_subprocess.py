"""Commands executed as subprocesses.
"""

import os


def exec(argv: 'Sequence', *,
         inputs: dict = None,
         outputs: dict = None,
         environment: dict = None,
         stdin=None,
         stdout: str = None,
         stderr: str = None,
         resources: dict = None
         ):
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
            >>> result = scalems.exec(('exe', '--origin', 1.0, 2.0, 3.0, '-f', my_filename), inputs={'infile': my_filename}, outputs={'outfile': 'exe.out'})
            >>> assert hasattr(result, 'file')
            >>> assert os.path.exists(result.file['outfile'].result())
            >>> assert hasattr(result, 'exitcode')

    """
    raise NotImplementedError()
