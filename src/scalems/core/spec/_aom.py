"""SCALE-MS abstract object model.

This module describes the roles of objects represented in the high level
workflow information. They will not be implemented directly.

This module is partly to illustrate the design and to serve as a guide and
record for development. The types may be used as the basis for PEP-544
Protocol definitions or PEP-544 type hints, but is not intended to dictate
class prototypes in the manner of https://docs.python.org/3/library/abc.html
abstract base classes.

Notes:
    At run time, type checking alone is not sufficient to determine
    data input/output compatibility without also checking shape.
"""

__all__ = []

import abc
import typing
from typing import Generic, Protocol

Label = typing.NewType('Label', str)
"""Valid label string for user-friendly workflow object identification.

Allows type annotation for static checks. Does not provide run time enforcement.

.. productionlist::
    labelcharacter: hyphen | underscore | `letter` | `integer`
    label: labelcharacter *labelcharacter

"""


class SchemaElement:
    """

    Example:
        The *result* member of a XType provides an identifier for another YType.
        The *result* member of a corresponding XInstance provides a Reference
        that resolves to a YType.
    """


class Identifier(typing.Hashable, Protocol):
    """Required information from identifiers in Scale-MS workflows."""
    attr_name: str  # Identifier attribute name.
    value_type: type  # Native type of the stored identifier value.

    @abc.abstractmethod
    def __str__(self) -> str:
        """Implementations may not be string based, but should specify their string representation."""
        raise NotImplementedError


class Fingerprint(Identifier, typing.SupportsBytes, abc.ABC):
    """Uniquely identify an object in terms of all known determinism.

    Sufficiently characterize the work graph node in terms of its input
    and specified transformation that, within the limits of numerical determinism,
    the output of the node can be fingerprinted regardless of whether the
    data is available.

    The data model will specify additional requirements on data representation,
    such as hash scheme.
    """
    attr_name = 'id'
    value_type = bytes

    @classmethod
    @abc.abstractmethod
    def __len__(cls) -> int:
        """Fingerprint types have static size."""
        raise NotImplementedError

    @abc.abstractmethod
    def hex(self) -> str:
        """String representations of Fingerprints are hexadecimal hash digests."""
        raise NotImplementedError


class ImplementationIdentifier(Identifier, abc.ABC):
    """Identify the module supporting an Object Type.

    Allows SCALE-MS to retrieve schema for instances, including
    input and output types, as well as necessary factories and
    serialization/deserialization support.
    """
    attr_name = 'implementation'


class Object(Protocol):
    """Base class for Scale-MS workflow objects."""
    identifier: Identifier
    """Uniquely identify the workflow object."""


class Implemented(Object, Protocol):
    """Annotate a type that uses ImplementationIdentifier."""
    identifier: ImplementationIdentifier


class Reference(Object, Protocol):
    """Reference another workflow object.

    Adds redirection, scoping, and slicing to other identifiers.
    """


class SupportsReference(Object, Protocol):
    """A type characteristic of objects that can be the targets of References."""
    identifier: Fingerprint


class Referent(Object, Protocol):
    """The target of a Reference.

    Following a Reference will yield a View of some sort.
    The view will have Object semantics from the client
    perspective, but may not map directly to a managed Object.
    (E.g. a slice or element).
    """
    identifier: Fingerprint


Resource = typing.TypeVar['Resource']


class ResourceType(Implemented, Protocol[Resource]):
    identifier: ImplementationIdentifier


class ResourceInstance(SupportsReference, Protocol[Resource]):
    identifier: Fingerprint

    @abc.abstractmethod
    def resource_type(self) -> ResourceType[Resource]:
        raise NotImplementedError


class DataInstance(ResourceInstance[Resource], Protocol):
    identifier: Fingerprint
    label: Label = None


class DataType(ResourceType[Resource], Protocol):
    identifier: ImplementationIdentifier
    data: typing.Union[bytes, Mapping]


class CommandType(ResourceType[Resource], Protocol):
    identifier: ImplementationIdentifier

    input: ResourceType
    result: ResourceType


class CommandInstance(ResourceInstance[Resource], Protocol):
    identifier: Fingerprint
    label: Label = None

    input: Reference
    result: Reference
    interface: typing.Optional[ResourceType]
