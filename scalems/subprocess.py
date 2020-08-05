"""Define the ScaleMS Subprocess command.

scalems.subprocess() is used to execute a program in one (or more) subprocesses.
It is an alternative to the built-in Python subprocess.Popen or asyncio.create_subprocess_exec
with extensions to better support ScaleMS execution dispatching and ensemble data flow.

In the first iteration, we can use dataclasses.dataclass to define input/output data structures
in terms of standard types. In a follow-up, we can use a scalems metaclass to define them
in terms of Data Descriptors that support mixed scalems.Future and native constant data types.

Examples:
    echo = make_subprocess_command(argv='/usr/bin/echo'
"""

import abc
import os
import typing
from dataclasses import dataclass
from pathlib import Path # We probably need a scalems abstraction for Path.

import scalems.types
from .core import OperationBase, OutputField, InputField


class _SubprocessBase:
    """Implementation helper."""


class AbstractSubprocessType(abc.ABC):
    """Characterize the interface of the thing that can produce Subprocess instances."""


class AbstractSubprocessInstance(abc.ABC):
    """Characterize the interface of the Subprocess nodes in the graph."""
    ...


def _make_subprocess_type(input_prototype: Type[SubprocessInput], result_prototype: ) -> AbstractSubprocessType: ...


# TODO: what is the mechanism for registering a command implementation in a new Context?
# TODO: What is the relationship between the command factory and the command type? Which parts need to be importable?

@scalems.utils.command(input_type=SubprocessInput, result_type=SubprocessResult)
class SubprocessA:
    def __instancecheck__(self, instance):
        assert isinstance(instance.inputs(), SubprocessInput)
        output_reference = instance.results()  # type: Reference
        referent = output_reference.referent()
        assert referent.uid() == instance.uid() or describe(referent).type is SubprocessResult
    def serialize(self) -> str:
        record = {''}


# scalems.N: placeholder for an integer constant (with optional constraints on possible values)
# scalems.Dynamic: placeholder for a value that will be determined during execution

class SubprocessB(OperationBase):
    argv = InputField(str, shape=(scalems.types.N(min=1),))
    inputs = InputField(scalems.types.Mapping(Path), shape=(1,), optional=True)
    outputs = InputField(scalems.types.Mapping(Path), shape=(1,), optional=True)
    stdin = InputField(str, shape=(scalems.Dynamic(),), optional=True)
    environment = InputField(scalems.types.Mapping(str), shape=(1,), optional=True)
    resources = InputField(scalems.types.Mapping(typing.Any), shape=(1,), optional=True)

    exitcode = OutputField(int, shape=(1,))
    stdout = OutputField(Path, shape=(1,), conditional=True)
    stderr = OutputField(Path, shape=(1,), conditional=True)
    file = OutputField(scalems.types.Mapping(Path), shape=(1,))

    # No implementation is provided here


Key = typing.Union[str, int, slice]


class ReferenceElement:
    def __init__(self, identifier: str, key: Key = None):
        self.identifier = identifier
        self.key = key

    def __str__(self):
        element = str(self.identifier)
        if self.key is not None:
            element += '['
            if isinstance(self.key, str):
                element += '"{}"'.format(self.key)
            elif isinstance(self.key, int):
                element += str(self.key)
            elif isinstance(self.key, slice):
                element += ':'.join(str(n) for n in [self.key.start, self.key.stop, self.key.step] if n is not None)
            else:
                raise TypeError('Bad key: {}'.format(repr(self.key)))
            element += ']'
        return element


class Reference:
    """
    Manage a reference as a discrete object.

    Support the Reference semantics described in the data model
    and the syntax described in serialization docs.

    Question: Do task/data handles know how to generate references
    to themselves? Note that referents are not necessarily uniquely
    referenced.
    """
    elements: typing.Sequence[ReferenceElement]

    def __init__(self, path: typing.Sequence[ReferenceElement]):
        self.elements = tuple([ReferenceElement(e.identifier, e.key) for e in path])

    def as_string(self) -> str:
        return '.'.join(str(element) for element in self.elements)

    @classmethod
    def from_string(cls, reference: str) -> 'Reference':
        elements = reference.split('.')
        path = []
        for element in elements:
            opening = element.find('[')
            if opening == -1:
                path.append(ReferenceElement(element))
            else:
                assert element.endswith(']')
                key = element[opening + 1: -1]
                if key.isdecimal():
                    key = int(key)
                else:
                    s = key.split(':')
                    assert len(s) < 3
                    if len(s) == 1:
                        key = s[0]
                    else:
                        start, stop, step = None, None, None
                        if s[0].isdecimal():
                            start = int(s[0])
                        if s[1].isdecimal():
                            stop = int(s[1])
                        if len(s) == 3:
                            if s[2].isdecimal():
                                step = int(s[2])
                        key = slice(start, stop, step)
                path.append(ReferenceElement(element[:opening], key))
        return cls(path)

    def describe(self) -> 'ResourceDescription':
        """Describe the final element in the reference path and its sliced shape."""
        raise NotImplementedError('Requires support from a WorkflowContext manager.')

    def referent(self) -> 'Referent':
        # We need to defer object ownership to the WorkflowContext.
        raise NotImplementedError('Requires support from a WorkflowContext manager.')


@dataclass
class SubprocessInput:
    argv: typing.Sequence[str]
    inputs: typing.Mapping[str, Path]
    outputs: typing.Mapping[str, Path]
    stdin: typing.Iterable[str]
    environment: typing.Mapping[str, typing.Union[str, None]]
    # For now, let's just always enable stdout/stderr
    # stdout: Optional[Path]
    # stderr: Optional[Path]
    resources: typing.Mapping[str, typing.Any]


@dataclass
class SubprocessResult:
    # file: Field(Path)
    # exitcode: Field(int)
    # TODO: Can we use None instead of os.devnull to indicate non-presence of stdout/stderr?
    exitcode: int
    stdout: Path
    stderr: Path
    file: typing.Mapping[str, Path]


class SubprocessResourceType:
    @classmethod
    def as_strings(cls):
        return ('scalems', 'subprocess')

    @classmethod
    def identifier(cls):
        return '.'.join(cls.as_strings())


# class Subprocess(CommandType):
class Subprocess:
    @classmethod
    def type(self):
        return SubprocessResourceType

    @classmethod
    def input_type(cls):
        return SubprocessInput

    @classmethod
    def result_type(cls):
        return SubprocessResult

    def __init__(self):
        self._bound_input = None
        self._result = None

    def input_collection(self):
        return self._bound_input

    def result(self):
        return self._result

    def dependencies(self):
        ...

    def uid(self):
        return ['0']*64

    def serialize(self) -> str:
        """Encode the task as a JSON record.

        Input and Result will be serialized as references.
        The caller is responsible for serializing existing records
        for bound objects, if they exist.
        """
        record = {}
        record['uid'] = self.uid()
        # "label" not yet supported.
        record['input'] = self._bound_input # reference
        record['result'] = self._result # reference
        raise NotImplementedError('To do...')

    @classmethod
    def deserialize(cls, record: str, context = None):
        """Instantiate a Subprocess Task from a serialized record.

        In general, records should only be deserialized into a WorkflowContext
        that manages a valid work graph, but for early testing, at least,
        we have some standalone use cases.
        """

        # The record may or may not have a bound result.
        # If there is a bound result, it should be added to the workgraph first.
        return cls()


assert issubclass(Subprocess, AbstractSubprocessType)


# Future work: scalems.partial to essentially "subclass" subprocess for particular commands
# for improved interfaces, better type checking, and cacheable bootstrapping.
#
# Prototypical examples.
#
#    @dataclass
#    class EchoInput:
#        arguments: Sequence[str]
#    echo = scalems.partial(scalems.subprocess, argv=scalems.extend_sequence(['/bin/echo'], Field('arguments', Sequence[str]))
#    text = TextFuture.from_string('hello')
#    instance = echo(arguments=[text])
#    outfile = instance.stdout.result()
#    with open(outfile, 'r') as fh:
#        output = fh.read()
#    assert output.startswith('hello')
#


