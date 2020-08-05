"""ScaleMS data model.

High level workflow representation:
    * scalems commands create typed task objects with associated typed results
    * high level task objects map clearly to the scalems task representation schema
    * Distinct workflow contexts can implement their own versions of standard scalems commands
    * high level task interfaces hide `asyncio` from users
    * propose dataclasses paradigm as a canonical short-hand for describing interfaces and replacing function signatures

Data and Task references do not appear substantially different. A Data type may
be considered to be a Task that performs an Identity operation.

Notes on DataTypes support:
    With Python 3.8, we get TypedDict support: https://www.python.org/dev/peps/pep-0589/
    Until then, typing.Mapping[K,V] cannot constraint the keyed items in the mapping.

    There is not yet an named abstract type for a mapping of unknown size.
    This implies that we need a special kind of Future behavior to allow a mapping with unknown
    size or keys to return something like a Future[GetItem] that produces either a look-up
    error or object of Any type at run time. We might be able to constrain the result
    to carry a required type or protocol so that it can throw either KeyError or TypeError
    depending on the run time result.

"""

import abc
import typing
from dataclasses import dataclass


# Simple command model:
# A command accepts a CommandInput instance or a combination of CommandInput Field keywords
# and additional key words for UIHelpers. The command returns a CommandInstance.
# A CommandInstance has an associated CommandType, InputType, ResultType, and
# Fingerprint factory by which the instance can be decomposed or abstracted, and
# which are used to implement the interface. The instance acts as a Future[ResultType]
# and binds to a Future[InputType], which is converted to a InputType[RuntimeContext]
# at run time.

# TODO: Abstract version of this class.
class UID(typing.Hashable):
    _sha256_digest: bytes

    def __init__(self, digest: bytes):
        if not isinstance(digest, bytes):
            raise TypeError('Expected digest as byte sequence.')
        if len(digest) != 32:
            raise ValueError('Expected a 256-bit SHA256 hash digest.')
        self._sha256_digest = bytes(digest)

    @classmethod
    def fromhex(cls, uid: str) -> 'UID':
        return UID(bytes.fromhex(uid))

    def __str__(self) -> str:
        return self._sha256_digest.hex()

    def __bytes__(self) -> bytes:
        return bytes(self._sha256_digest)

    def __int__(self) -> int:
        return int(self._sha256_digest)

    def __hash__(self) -> int:
        return int(self._sha256_digest)

    def __eq__(self, o: bytes) -> bool:
        error_message = 'UIDs are 256-bit bytes-like objects.'
        if not isinstance(o, typing.SupportsBytes):
            raise TypeError(error_message)
        other = bytes(o)
        if len(other) != 32:
            raise TypeError(error_message)
        return self._sha256_digest == other


class ResourceType(abc.ABC):
    """Represent a ScaleMS Command or data type."""
    def as_strings(self) -> typing.Sequence[str]:
        """Return a sequence of strings, starting with the outer namespace and ending with the type name."""
        ...

    @classmethod
    @abc.abstractmethod
    def identifier(cls):
        """Implementation identifier.

        Get the implementing module or factory function.
        """
        ...


class FieldLabel(str, abc.ABC):
    def __instancecheck__(self, instance):
        if isinstance(instance, str):
            # apply regex.
            ...
        # fail check


# Data dimensions are fundamentally of fixed integral size.
# Some special placeholders will evolve to allow for dimensions of unknown size,
# but the scenarios under which this is possible will need to be discussed and refined.
DimensionSize = typing.Union[int]


class ResourceDescription:
    def shape(self) -> typing.Tuple[DimensionSize]:
        ...

    def type(self) -> ResourceType:
        ...



class Referent(typing.Protocol):
    def uid(self) -> UID:
        ...


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

    def describe(self) -> ResourceDescription:
        """Describe the final element in the reference path and its sliced shape."""
        raise NotImplementedError('Requires support from a WorkflowContext manager.')

    def referent(self) -> Referent:
        # We need to defer object ownership to the WorkflowContext.
        raise NotImplementedError('Requires support from a WorkflowContext manager.')


class TypeField:
    name: FieldLabel
    value: ResourceDescription


class InstanceField:
    name: FieldLabel
    value: Reference


@dataclass
class Descriptor:
    """Describe an object reference in a ScaleMS workflow."""
    shape: typing.Tuple[int]
    ref_type: typing.Sequence[str]


# TypeVar for input type placeholder.
# TODO: constrain to a mapping of field labels to Descriptions.
InputType = typing.TypeVar('InputType')


# TODO: constrain to a collection matching the structure described by InputType
# TODO: This is probably an abstract class defined in terms of InputType, not a TypeVar.
InputCollection = typing.TypeVar('InputCollection')


# TypeVar for result type placeholder.
# TODO: constrain to a mapping of field labels to Descriptions.
ResultType = typing.TypeVar('ResultType')


# TODO: constrain to a collection matching the structure described by ResultType
# TODO: This is probably an abstract class defined in terms of ResultType, not a TypeVar.
ResultCollection = typing.TypeVar('ResultCollection')


class InstanceType(typing.Protocol):
    def uid(self) -> UID: ...


T = typing.TypeVar('T')

class Future(abc.ABC, typing.Generic[T]):
    def result(self) -> T:
        ...


class CommandType(abc.ABC, typing.Generic[InputType, ResultType]):
    """An abstract command that can be used to create concrete command instances.

    The abstract command is partly defined in terms of its InputType and ResultType.

    The Instance, or the task that can be created, is further specialized by the
    TargetContext. Individual inputs may be generic in terms of their (Source)Context,
    and the task factory will dispatch Input instance construction in terms of these SourceContexts.
    """
    @abc.abstractmethod
    def type(self) -> typing.Type[ResourceType]:
        ...

    @classmethod
    @abc.abstractmethod
    def input_type(cls) -> typing.Type[InputType]:
        ...

    @abc.abstractmethod
    def input_collection(self) -> Future[InputType]:
        ...

    @classmethod
    @abc.abstractmethod
    def result_type(cls) -> typing.Type[ResultType]:
        ...

    @abc.abstractmethod
    def result(self) -> ResultType:
        ...

    def serialize(self) -> str:
        ...

    @classmethod
    def deserialize(cls: T, encoded: str) -> T:
        ...


# TODO: Describe a scheme for declaring conversions, "compatible data sources", or "available data types".
# The most obvious option would be to declare some single-dispatch functions at appropriate module scopes
# and allow new modules to register overloads.
# Note: Double-dispatch syntax can be simplified in Python 3.8 in terms of single-dispatch class methods.
#
# Example:
#     # Register participation in a generic protocol.
#     from scalems.simulate import get_trajectory
#
# Example:
#     # Register the ability to provide data of a certain type.
#     # When expecting an argument of type gromacs.SimulationInput, call `func(arg: MyType)`
#     # through a dispatcher once `func` is registered for dispatching on `MyType`
#     from scalems.wrappers import gromacs
#     gromacs.SimulationInput.register_convert_from(MyType, func)
#
# Example:
#     # Register the ability to consume data of a certain type.
#     class MyInput:
#         trajectory = InputField(Path)
#     MyInput.trajectory.register(gromacs.Trajectory, my_gromacs_handler)
#
#
# We can probably hide at least some implementation behind decorators for user convenience and developer flexibility.
#
# Example:
#     inputfile = scalems.InputField(Path)
#
#     @scalems.wrappers.gromacs.trajectory_source
#     @scalems.wrappers.coordinates_source
#     class TrajReader(scalems.Command):
#         trajectory = inputfile
#
#         # Easier to implement after Python 3.8
#         @trajectory.register
#         @classmethod
#         def _(self, traj: scalems.wrappers.gromacs.Trajectory):
#             # Perform input handling for *trajectory* of type gromacs.Trajectory
#             ...

