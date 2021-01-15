"""Interfaces and abstractions supporting the SCALE-MS data model and concurrency model.

The primary purpose of this module is for documentation and static type check support.

Implementation details should be deferred to scalems.core.support.

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
As opposed to typing.Mapping[K,V], typing.TypedDict allows to constrain the keyed items in the mapping.

There is not yet an named abstract type for a mapping of unknown size.
This implies that we need a special kind of Future behavior to allow a mapping with unknown
size or keys to return something like a Future[GetItem] that produces either a look-up
error or object of Any type at run time. We might be able to constrain the result
to carry a required type or protocol so that it can throw either KeyError or TypeError
depending on the run time result.

References:
    Support for type hints: https://docs.python.org/3/library/typing.html
    Structural subtyping (static duck-typing): https://www.python.org/dev/peps/pep-0544/
    Distributing and packaging type information: https://www.python.org/dev/peps/pep-0561/

"""

__all__ = ['Future', 'InputType', 'Static']

import typing
from typing import Protocol

from .basictypes import DimensionSize


class UID(typing.SupportsInt,
          typing.SupportsBytes,
          typing.Hashable,):
    @classmethod
    def from_string(cls, identity: str) -> 'UID':
        ...

    @classmethod
    def from_int(cls, identity: int) -> 'UID':
        ...

    @classmethod
    def from_bytes(cls, identity: bytes) -> 'UID':
        ...


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


# class Reference:
#     def as_string(self) -> str:
#         ...
#
#     @classmethod
#     def from_string(cls, reference: str) -> 'Reference':
#         ...
#
#     def describe(self) -> ResourceDescription:
#         ...
#
#     def referent(self) -> Referent:
#         ...


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


class CommandType(abc.ABC, typing.Generic[InputType, ResultType]):
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


class Resource(Protocol):
    """Basic workflow resource type."""


class Reference:
    """Allow a WorkflowItem to refer to another item in the workflow."""


class Description:
    """Describe a referentiable workflow object."""


ResourceT = typing.TypeVar('ResourceT', bound=Resource)


class Future(Protocol[ResourceT]):
    ...


class Static(Protocol[ResourceT]):
    ...


class InputType(typing.Union[Future[ResourceT], Static[ResourceT]]):
    """Generic annotation for commands consuming Resource subtypes.

    Example::

        CommandInput = InputType[CommandResources]

        def command(arg: CommandInput):
            ...

        inp: Future[CommandResources]

        cmd = command(inp)

    """
    ...


class ResourceDescription:
    def shape(self) -> typing.Tuple[DimensionSize]:
        ...

    def type(self) -> ResourceType:
        ...



class Referent(typing.Protocol):
    def identity(self) -> UID:
        ...


Key = typing.Union[str, int, slice]




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
    def identity(self) -> UID: ...


T = typing.TypeVar('T')

class Future(abc.ABC, typing.Generic[T]):
    def result(self) -> T:
        ...


class CommandType(typing.Protocol[InputType, ResultType]):
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

