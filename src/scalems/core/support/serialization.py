"""Provide encoding and decoding support for serialized workflow representations.

Core support for ResourceType, conversion, and representation.

Define representations for resource types in SCALE-MS.

Provide resolution from representations to implementations.

Provide encoding and decoding between Workflow resource references and basic Python objects.

This submodule defines a resolution scheme to locate SCALE-MS support for named resource types.
The look-up scheme can be supplemented or short-circuited through direct registration of
support functions and/or concrete classes.

Reference https://docs.python.org/3/library/json.html#json.JSONDecoder and
https://docs.python.org/3/library/json.html#py-to-json-table describe the
trivial Python object conversions.
The core SCALE-MS encoder / decoder needs to manage the conversion of
additional types (scalems or otherwise, e.g. *bytes*) to/from these basic
Python types.

For JSON, we can provide an encoder for the *cls* parameter of json.dumps()
and we can provide a key-value pair processing dispatcher to the *object_pairs_hook*
parameter of json.loads()

We may also implement serialization schemes for document formats other than
JSON, such as the CWL schema.

ResourceType resolution:
    scalems resolves named types in the following way.
    1. The scalems.codec package resource group is checked for a mapping of identifier to implementation object. (TODO: Define the interface of an implementation class or module.) Note that the package resources are scanned at module import to speed up run-time lookups.
    2. This module consults its current state for registered implementations.
    3. The type identifier is interpreted as an importable Python entity. The module attempts to import an implementation.

Implementation interface:
    In a first pass, we will assume that negotiation between SCALE-MS and implementation code is handled sufficiently
    through regular import processing, and we just import the implementation entity. In the long run, we should account
    for the fact that a SCALE-MS compatible package may need to avoid the import-time overhead of exposing SCALE-MS hooks
    on all imports, and SCALE-MS should call a normative member function of the imported class or module. (``scalems_init()``?)
    TODO: Document "A class or module provides a SCALE-MS resource type implementation by providing the following interface."

    As a helper, we provide the decorators ``resource`` and ``data`` and ``command`` decorators to update the resource type
    registry with implementation classes.

"""
from __future__ import annotations

__all__ = ['BasicSerializable', 'decode', 'encode', 'Shape', 'TypeIdentifier']

import abc
import collections.abc
import contextvars
import functools
import hashlib
import json
import logging
import os
import sys
import types
import typing
import uuid
import warnings
import weakref

from scalems.core.exceptions import APIError
from scalems.core.exceptions import InternalError
from scalems.core.exceptions import MissingImplementationError
from scalems.core.exceptions import ProtocolError

if sys.version_info.major > 3 or sys.version_info.minor >= 9:
    Dict = dict
    List = list
    Tuple = tuple
else:
    assert sys.version_info.major == 3
    from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

# TODO: Use a URL to the schema or specification.
NAMESPACE_SCALEMS: uuid.UUID = uuid.uuid5(uuid.NAMESPACE_DNS, 'scalems.org')

TypeRepr = typing.Union['NamedIdentifier', List[str], Tuple[str], type, str]
"""Represent a SCALE-MS Resource Type.

A TypeIdentifier may be constructed from several representations, including
period-delimited strings, sequences of strings, importable Python objects,
and NamedIdentifier instances.
"""

InputT = typing.TypeVar('InputT')
ResultT = typing.TypeVar('ResultT')
ResourceT = typing.TypeVar('ResourceT')


class ResourceType(typing.Protocol[InputT, ResultT]):
    """Protocol for interfaces to workflow resources.

    If the resource provides data(), it must also provide shape().

    Resources implementing items in the work graph should provide label().

    Workflow items representing concrete data should use a checksum-based identifier,
    and should provide a fingerprint() class method for generic use to compare arbitrary
    representations of the named type.
    """
    def type(self) -> TypeIdentifier:
        """Get the registered resource type.

        Note that multiple classes are allowed to implement a named type.
        If dispatching is required to choose a particular implementation when
        instantiating resources, the corresponding ResourceFactory should be
        implemented generically.
        """
        ...

    def identity(self) -> Identifier:
        """Identify the managed workflow resource."""
        ...

    def result(self) -> ResultT:
        ...

    # Let's see if we can avoid requiring this by relying on `resolve()`.
    # @classmethod
    # def factory(cls) -> ResourceFactory[InputT, ResultT]:
    #     ...


class WorkflowItemType(ResourceType[InputT, ResultT], typing.Protocol[InputT, ResultT]):
    """Extended Resource protocol for workflow items.

    If the resource provides data(), it must also provide shape().

    Resources implementing items in the work graph should provide label().

    Workflow items representing concrete data should use a checksum-based identifier,
    and should provide a fingerprint() class method for generic use to compare arbitrary
    representations of the named type.
    """
    @classmethod
    def fingerprint(cls, obj) -> bytes:
        ...

    def data(self):
        ...

    def shape(self):
        ...

    def label(self) -> str:
        ...


class ResourceFactory(typing.Protocol[InputT, ResultT]):
    """Define the signature of functions and function objects for obtaining handles to workflow resources."""
    def __call__(self, input: InputT, *args, context=None, label=None, **kwargs) -> ResourceType[InputT, ResultT]:
        """Create a workflow resource and return a handle."""
        ...


class WorkflowItemFactory(ResourceFactory[InputT, ResultT], typing.Protocol[InputT, ResultT]):
    """Define the signature of functions and function objects for obtaining handles to workflow resources."""
    def __call__(self, input: typing.Union[InputT, EncodedItem], *args, context=None, label=None, **kwargs) -> WorkflowItemType[InputT, ResultT]:
        """Add an item to a workflow and return a handle.

        Required overloads include the decoder (from dict schema) and the standard input type for the resource, if any.

        Additional overloads are allowed to provide user-convenience.
        """
        ...


_resource_factories = contextvars.ContextVar


def _initialize_resource_types():
    """Initialize the module state.

    Set up the module state for maintaining the type registry. Then apply any documented procedure(s) for
    identifying extension software.
    """
    ...


# Boot-strap the resource type registry.
logger.info('Preparing named resource type resolution for SCALE-MS compatible resources.')
_initialize_resource_types()


FactoryT = typing.TypeVar('FactoryT', bound=ResourceFactory)

# Principle resource type resolution machinery.
# TODO: Consder the model of https://docs.python.org/3/library/codecs.html for a normative and complete resource module.
def resolve(typeid: TypeRepr) -> FactoryT:
    ...


@typing.overload
def resource(implementation: ResourceT, /, *, typeid: TypeRepr = None) -> ResourceT:
    ...


@typing.overload
def resource(*, typeid: TypeRepr) -> typing.Callable[[ResourceT], ResourceT]:
    ...


def get_type(implementation: typing.Type[ResourceT]) -> TypeIdentifier:
    """Get a SCALE-MS TypeIdentifier for an implementation class.

    Resolution rules:
        ...
    """
    ...


def register(typeid: TypeIdentifier, implementation: typing.Type[ResourceT]):
    """Register an implementation for a SCALE-MS resource type.

    Clarify:
        Simple invocation with an implementation class initializes a generic factory with
        the class constructor as the default implementation.
        Later calls can add dispatching for the factory. Presumably by registering with the singledispatch function
        for the InputT, EncodedItem, and then other distinguishable types for arg[0].
        (The provided decorators should take care of the first two overloads.)
    """
    ...


def resource(*args, **kwargs):
    """Register and return a resource implementation.

    May be used as a decorator.
    """
    if len(args) > 1:
        raise TypeError('Wrong number of positional arguments. Expected zero or one.')
    for kw in kwargs:
        if kw != 'typeid':
            raise TypeError(f'{kw!r} is an invalid keyword argument for resource()')

    if 'typeid' in kwargs:
        typeid = TypeIdentifier.copy_from(kwargs['typeid'])
    else:
        typeid = None

    def wrap(implementation: typing.Type[ResourceType], typeid=typeid):
        if typeid is None:
            typeid = get_type(implementation)
        register(typeid, implementation)
        return implementation

    if len(args) == 1:
        # Assume we were called as a regular decorator.
        implementation = typing.cast(ResourceType, args[0])
        return wrap(implementation)
    else:
        assert len(args) == 0
        # Assume we were called as a parameterized decorator (with parentheses). Return a decorator.
        if 'typeid' not in kwargs:
            raise TypeError('Missing key word argument. Parameterized decorator requires "typeid".')
        return wrap


FingerprintHash = typing.NewType('FingerprintHash', bytes)
"""The fingerprint hash is a 32-byte sequence containing a SHA256 digest."""

IdentifierT = typing.TypeVar('IdentifierT', bound='Identifier')

#############################
# TODO: move (to _detail.py?)
@typing.runtime_checkable
class Identifier(typing.Hashable, typing.Protocol):
    """SCALE-MS object identifiers support this protocol.

    Identifiers may be implemented in terms of a hashing scheme, RFC 4122 UUID,
    or other encoding appropriate for the scope of claimed uniqueness and
    reusability (cacheable).

    Namespace UUIDs are appropriate for strongly specified names, such as operation implementation identifiers.
    The 48 bits of data are sufficient to identify graph nodes at session scope.
    At workflow scope, we need additional semantics about what should persist or not.

    Concrete data warrants a 128-bit or 256-bit checksum.


    """
    scope: str
    """Scope in which the Identifier is effective and unique."""
    # TODO: Use an enum that is part of the API specification.

    reproducible: bool
    """Whether results will have the same identity if re-executed, such as due to missing cache."""

    concrete: bool
    """Is this a concrete object or something more abstract?"""

    @abc.abstractmethod
    def bytes(self) -> typing.Union[bytes, typing.SupportsBytes]:
        """The core interface provided by Identifiers is a consistent bytes representation of their identity.

        Note that the identity (and the value returned by self.bytes()) must be immutable for
        the life of the object.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __str__(self) -> str:
        """Represent the identifier in a form suitable for serialized records.

        All Identifier subclasses must explicitly define the string representation,
        but may ``return super().__str__(self)`` for a suitable default
        (a hexadecimal encoding of the core data).

        By default, the string representation is the basis for the stub used
        for filesystem objects. To change this, override the __fspath__() method.
        """
        return hex(self)

    @classmethod
    @abc.abstractmethod
    def copy_from(cls: typing.Type[IdentifierT], obj) -> IdentifierT:
        """Produce a new instance of the Identifier from another object.

        Subclasses will probably implement this as a singledispatchmethod to
        provide a generic alternative to __init__.

        At the very least, implementations should support
        the encoded form of the Identifier and other instances of the Identifier.

        Note that copies of Identifiers must compare equal.
        """
        raise NotImplementedError

    def encode(self) -> BaseEncodable:
        """Get a canonical encoding of the identifier as a native Python object.

        This is the method that will be used to produce serialized workflow records.

        By default, the string representation (self.__str__()) is used.
        Subclasses may override, as long as suitable decoding is possible and provided.
        """
        return str(self)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.bytes() == self.bytes()

    def __hash__(self) -> int:
        # Note that the result of the `hash()` built-in is truncated to
        # the size of a `Py_ssize_t`, but two objects that compare equal must
        # return the same value for `__hash__()`, so we return the full value.
        return self.__index__()

    def __fspath__(self) -> str:
        """Get a representation suitable for naming a filesystem object."""
        path = os.fsencode(str(self))
        return str(path)

    def __bytes__(self) -> bytes:
        """Get the network ordered byte sequence for the raw identifier."""
        return bytes(self.bytes())

    def __index__(self) -> int:
        """Support integer conversions."""
        return int.from_bytes(self.bytes(), 'big')


class EphemeralIdentifier(Identifier):
    """Process-scoped UUID based identifier.

    Not reproducible. Useful for tracking objects within a single process scope.
    """

    def __init__(self, node=None, clock_seq=None):
        self._data: uuid.UUID = uuid.uuid1(node, clock_seq)

    def bytes(self):
        return self._data.bytes

    @classmethod
    def copy_from(cls: typing.Type[EphemeralIdentifier], obj) -> EphemeralIdentifier:
        if isinstance(obj, cls):
            value = cls()
            value._data = uuid.UUID(bytes=obj._data.bytes)
            return value
        if isinstance(obj, str):
            try:
                value = cls()
                value._data = uuid.UUID(hex=obj)
                return value
            except Exception as e:
                raise APIError(f'Bad encoding of {cls!r}: {obj}')
        raise ValueError(f'No decoder for {cls} represented as {obj!r}.')

    def __str__(self) -> str:
        return str(self._data)


class NamedIdentifier(Identifier):
    """A name with strong identity semantics, represented with a UUID."""

    # TODO: facility to annotate scope
    # TODO: facility to annotate reproducibility
    # TODO: facility to indicate whether this is a reference to concrete data or not.
    def __init__(self, nested_name: typing.Sequence[str]):
        try:
            if isinstance(nested_name, (str, bytes)):
                raise TypeError('Wrong kind of iterable.')
            self._name_tuple = tuple(str(part) for part in nested_name)
        except TypeError as e:
            raise TypeError(f'Could not construct {self.__class__.__name__} from {repr(nested_name)}')
        else:
            self._data = uuid.uuid5(NAMESPACE_SCALEMS, '.'.join(self._name_tuple))
        # TODO: The instance should track a context in which the uuid can be resolved.

    def bytes(self):
        return self._data.bytes

    def __str__(self) -> str:
        return str(self._data)

    def encode(self) -> BaseEncodable:
        return self._name_tuple


class ResourceIdentifier(Identifier):
    # TODO: facility to annotate scope
    # TODO: facility to annotate reproducibility
    # TODO: facility to indicate whether this is a reference to concrete data or not.
    def __init__(self, fingerprint: bytes):
        self._data = bytes(fingerprint)
        # Expecting a 256-bit SHA256 hash digest
        if len(self._data) != 32:
            raise InternalError(f'Expected a 256-bit hash digest. Got {repr(fingerprint)}')

    def bytes(self) -> bytes:
        return bytes(self._data)

    def __str__(self) -> str:
        return self.bytes().hex()

    @classmethod
    def copy_from(cls: typing.Type[IdentifierT], obj) -> IdentifierT:
        if isinstance(obj, ResourceIdentifier):
            return cls(obj._data)
        if isinstance(obj, str):
            try:
                return cls(bytes.fromhex(obj))
            except Exception as e:
                raise APIError(f'Bad encoding of {cls!r}: {obj}')
        raise ValueError(f'No decoder for {cls} represented as {obj!r}.')



class TypeIdentifier(NamedIdentifier):
    def name(self):
        return '.'.join(self._name_tuple)

    def scoped_name(self):
        return self._name_tuple

    @classmethod
    def copy_from(cls, typeid: TypeRepr) -> 'TypeIdentifier':
        """Create a new TypeIdentifier instance describing the same type as the source.

        .. todo:: We need a generic way to determine the (registered) virtual type of an object, but that doesn't belong here.
        """
        if isinstance(typeid, NamedIdentifier):
            # Copy from a compatible object.
            return cls(typeid._name_tuple)
        if isinstance(typeid, (list, tuple)):
            # Create from the usual initialization parameter type.
            return cls(typeid)
        if isinstance(typeid, type):
            # Try to generate an identifier based on a defined class.
            #
            # Consider disallowing TypeIdentifiers for non-importable types.
            # (Requires testing and enforcement.)
            # Consider allowing class objects to self-report their type.
            if typeid.__module__ is not None:
                fully_qualified_name = '.'.join((typeid.__module__, typeid.__qualname__))
            else:
                fully_qualified_name = str(typeid.__qualname__)
            return cls.copy_from(fully_qualified_name)
        if isinstance(typeid, str):
            # Conveniently try to convert string representations back into the namespace sequence representation.
            # TODO: First check if the string is a UUID or other reference form for a registered type.
            return cls.copy_from(tuple(typeid.split('.')))
        # TODO: Is there a dictionary form that we should allow?


#############################

json_base_encodable_types: typing.Tuple[type, ...] = (dict, list, tuple, str, int, float, bool, type(None))
json_base_decoded_types: typing.Tuple[type, ...] = (dict, list, str, int, float, bool, type(None))

BaseEncodable = typing.Union[dict, list, tuple, str, int, float, bool, None]
BaseDecoded = typing.Union[dict, list, str, int, float, bool, None]


class Shape(tuple):
    """Describe the data shape of a SCALEMS object."""

    def __new__(cls, elements: typing.Iterable):
        return super().__new__(cls, elements)

    def __init__(self, elements: typing.Iterable):
        """Initial implementation requires a sequence of integers.

        Software requirements include symbolic elements, TBD.
        """
        try:
            es = tuple(e for e in elements)
        except TypeError as e:
            raise e
        if len(es) < 1 or any(not isinstance(e, int) for e in es):
            raise TypeError('Shape is a sequence of 1 or more integers.')


# It could make sense to split the codec for native-Python encoding from the
# (de)serialization code in the future...

class SchemaDict(typing.TypedDict):
    """Schema for the member that labels an object's schema.

    This is just a type hint for the moment. The specification can be strengthened
    in the core data model and module constants provided for the schema comprising
    the full specification.

    Notes:
        * Python 3.9 provides a "frozenmap"
        * Consider a namedtuple, dataclass, or similar and make dict interconversion secondary.
        * We should clarify object model policies such as the invariance/covariance/contravariance
          of members through subtyping.

    TODO: Allow equality check
    TODO: Actually integrate with object support metaprogramming in the package.
    """
    spec: str
    name: str


class SymbolicDimensionSize(typing.TypedDict):
    DimensionSize: str


ShapeElement = typing.Union[int, SymbolicDimensionSize]


class FieldDict(typing.TypedDict):
    """Members of the *fields* member of a ResourceType."""
    schema: SchemaDict
    type: typing.List[str]
    shape: typing.List[ShapeElement, ...]


FieldsType = typing.Mapping[str, FieldDict]


class TypeDict(typing.TypedDict):
    """Express the expected contents of a dictionary-based type description."""
    schema: SchemaDict
    implementation: typing.List[str]
    fields: FieldsType


EncodedDict = Dict[str, 'EncodedElement']
T = typing.TypeVar('T', bound=BaseEncodable)
GenericEncodedSequence = typing.Union[List[T], Tuple[T]]
EncodedSequence = typing.Union[List['EncodedElement'], Tuple['EncodedElement']]

EncodedElement = typing.Union[str, int, float, bool, None, EncodedDict, EncodedSequence]
"""A member of an EncodedItem.

An encoded object contains only encoded elements.
BaseEncodable objects that are not Containers are valid EncodedElements.
BaseEncodable Containers are valid EncodedElements if all contained objects
are EncodedElements.
"""

# Note: Very soon we will need to encode additional element types as strings or sequences of strings.
EncodedShapeElement = int


class EncodedItem(typing.TypedDict):
    """A native Python dictionary satisfying the serialization schema.

    An Encoded object and all of its nested data are BaseEncodable.
    """
    label: typing.Optional[str]
    identity: str
    type: typing.Union[List[str], Tuple[str]]
    shape: typing.Union[List[EncodedShapeElement], Tuple[EncodedShapeElement]]
    data: EncodedElement


DispatchT = typing.TypeVar('DispatchT')


class PythonEncoder:
    """Encode SCALE-MS objects as basic Python data that is easily serialized.

    Extend the JSONEncoder for representations in the SCALE-MS data model by
    passing to the *default* argument of ``json.dumps()``,
    but note that it will only be used for objects that JSONEncoder does not already
    resolve.

    Note that json.dump and json.dumps only use the *default* call-back when the *cls* encoder does not
    have an implementation for an object type. To preempt standard processing by the JSONEncoder,
    you must provide a *cls* that overrides the encoding methods as documented at
    https://docs.python.org/3/library/json.html#json.JSONEncoder.encode to produce a string.
    This is _not_ what the *encode* method of this class does.

    Alternatively, encode object(s) first, and pass the resulting Python object to a regular call to json.dumps.
    """
    # Note that the following are equivalent.
    #     json.loads(s, *, cls=None, **kw)
    #     json.JSONDecoder(**kw).decode(s)
    # Note that the following are equivalent.
    #     json.dumps(obj, *, cls=None, **kw)
    #     json.JSONEncoder(**kw).encode(obj)

    # We use WeakKeyDictionary because the keys are likely to be classes,
    # and we don't intend to extend the life of the type objects (which might be temporary).
    _dispatchers: typing.ClassVar[typing.MutableMapping[
        typing.Type[DispatchT], typing.Callable[[DispatchT], BaseEncodable]]] = weakref.WeakKeyDictionary()

    # TODO: Confirm that behavior is consistent with expectations of "virtual subclasses" and functools.singledispatch().register
    @classmethod
    def register(cls, *, dtype: typing.Type[DispatchT], handler: typing.Callable[[DispatchT], BaseEncodable]):
        # Note that we don't expect references to bound methods to extend the life of the type.
        # TODO: confirm this assumption in a unit test.
        if not isinstance(dtype, type):
            raise TypeError('We use `isinstance(obj, dtype)` for dispatching, so *dtype* must be a `type` object.')
        if dtype in cls._dispatchers:
            raise ProtocolError(f'Encodable type {dtype} appears to be registered already.')
        cls._dispatchers[dtype] = handler

    @classmethod
    def unregister(cls, dtype: typing.Type[DispatchT]):
        # As long as we use a WeakKeyDictionary, explicit unregistration should not be necessary.
        del cls._dispatchers[dtype]

    @classmethod
    def encode(cls, obj) -> BaseEncodable:
        """Convert an object of a registered type to a representation as a basic Python object."""
        # Currently, we iterate because we may be using abstract types for encoding.
        # If we find that we are using concrete types and/or we need more performance,
        # or if we just find that the list gets enormous, we can inspect the object first
        # to derive a dtype key that we can look up directly.
        # Warning: we should be careful not to let objects unexpectedly match multiple entries.
        for dtype, dispatch in cls._dispatchers.items():
            if isinstance(obj, dtype):
                return dispatch(obj)
        if type(obj) in json_base_encodable_types:
            return obj
        raise TypeError(f'No registered dispatching for {repr(obj)}')

    def __call__(self, obj) -> BaseEncodable:
        return self.encode(obj)


class UnboundObject(abc.ABC):
    """A prototypical instance of a workflow item not bound to a workflow.

    Generally, SCALEMS objects are items in a managed workflow.
    """
    def encode(self) -> BaseEncodable:
        ...


class PythonDecoder:
    """Convert dictionary representations to SCALE-MS objects for registered types.

    Dictionaries are recognized as SCALE-MS object representations with a minimal heuristic.

    If the object (dict) contains a *'schema'* key, and the value
    is a dict, the *'spec'* member of the dict is retrieved. If the *'spec'* member exists and
    names a recognized schema specification, the object is dispatched according to the schema
    specification.

    Otherwise, if the object contains a *'type'* key, identifying a recognizable registered type,
    the object is dispatched to the decoder registered for that type.

    For more information, refer to the :doc:`serialization` and :doc:`datamodel` documentation.
    """
    # TODO: Consider flattening this class into a module namespace. ref: codec.py
    # TODO: Consider specifying a package metadata resource group to allow packages to register
    #       additional schema through an idiomatic plugin system.
    # Refs:
    #  * https://packaging.python.org/guides/creating-and-discovering-plugins/
    #  * https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html#dynamic-discovery-of-services-and-plugins

    _dispatchers: typing.MutableMapping[
        TypeIdentifier,
        typing.Callable] = dict()

    # Depending on what the callables are, we may want a weakref.WeakValueDictionary() or we may not!

    # TODO: Confirm that behavior is consistent with expectations of "virtual subclasses" and functools.singledispatch().register
    @classmethod
    def register(cls, *, typeid: TypeIdentifier, handler: typing.Callable):
        # Normalize typeid
        typeid = TypeIdentifier.copy_from(typeid)
        if typeid in cls._dispatchers:
            raise ProtocolError('Type appears to be registered already.')
        cls._dispatchers[typeid] = handler

    @classmethod
    def unregister(cls, typeid: TypeIdentifier):
        del cls._dispatchers[typeid]

    @classmethod
    def get_decoder(cls, typeid) -> typing.Union[None, typing.Callable]:
        # Normalize the type identifier.
        try:
            identifier = TypeIdentifier.copy_from(typeid)
            typename = identifier.name()
        except TypeError:
            try:
                typename = str(typeid)
            except TypeError:
                typename = repr(typeid)
            identifier = None
        # Use the (hashable) normalized form to look up a decoder for dispatching.
        if identifier is None or identifier not in cls._dispatchers:
            raise TypeError('No decoder registered for {}'.format(typename))
        return cls._dispatchers[identifier]

    # Note: I don't think we need an UnboundObject.
    # TODO: Decode exclusively in terms of a WorkflowManager.
    #  Replace UnboundObject with a Protocol that indicates an object has been validated.
    #  Consider overloads to distinguish the signatures for nested BaseDecoded data versus
    #  the TypedDict that indicates the final prototype of an almost-decoded ScaleMS object.
    @classmethod
    def decode(cls, obj) -> typing.Union[Encodable, BaseDecoded]:
        """Create unbound SCALE-MS objects from their basic Python representations.

        We assume this is called in a bottom-up manner as a nested record is deserialized.

        Unrecognized objects are returned unaltered because they may be members
        of an enclosing object with appropriate dispatching.

        .. todo:: Consider where to register transcoders for compatible/virtual types.
                  E.g. Infer np.array(..., dtype=int) -> scalems.Integer
                  This is a small number of cases, since we can lean on the descriptors in the buffer protocol.
        """
        if not isinstance(obj, dict):
            # Probably don't have any special handling for such objects until we know what they are nested in.
            ...
        else:
            assert isinstance(obj, dict)
            if 'schema' in obj:
                # We currently have very limited schema processing.
                try:
                    spec = obj['schema']['spec']
                except KeyError:
                    spec = None
                if not isinstance(spec, str) or spec != 'scalems.v0':
                    # That's fine...
                    logger.info('Unrecognized *schema* when decoding object.')
                    return obj
                if 'name' not in obj['schema'] or not isinstance(obj['schema']['name'], str):
                    raise InternalError('Invalid schema.')
                else:
                    schema = obj['schema']['name']
                # Dispatch the object...
                ...
                raise MissingImplementationError(
                    'We do not yet support dynamic type registration through the work record.')

            if 'type' in obj:
                # Dispatch the decoding according to the type.
                try:
                    dispatch = cls.get_decoder(obj['type'])
                except TypeError:
                    dispatch = BasicSerializable.decode
                if dispatch is not None:
                    return dispatch(obj)
        # Just return un-recognized objects unaltered.
        return obj

    def __call__(self, obj) -> typing.Union[Encodable, BaseDecoded]:
        return self.decode(obj)


# TODO: These seem like they should be module functions of a `codec` submodule.
encode = PythonEncoder()
decode = PythonDecoder()

# TODO: use stronger check for UID, or bytes-based objects.
encode.register(dtype=bytes, handler=bytes.hex)
encode.register(dtype=os.PathLike, handler=os.fsdecode)

# Note that the low-level encoding/decoding is not necessarily symmetric because nested objects may be decoded
# according to the schema of a parent object.
# decode.register()

def decodes_to(typeid: TypeRepr) -> type:
    """Find the class object for a SCALE-MS dtype.

    Check the registered decoders for the given typeid.
    If the dtype is not registered, try to import.

    Returns None if the typeid is not decodable.
    """
    # TODO: This seems like it should be a module function of a `codec` submodule.
    raise MissingImplementationError('To do...')


class TypeDataDescriptor:
    """Implement the *dtype* attribute.

    The TypeDataDescriptor object is instantiated to implement the
    BasicSerializable.base_type dynamic attribute.

    Attributes:
        name: Name of the attribute provided by the data descriptor.
        base: TypeIdentifier associated with the Python class.
        attr_name: the name of the instance data member used by this descriptor for storage.

    *name* can be provided at initialization, but is overridden during class
    definition when TypeDataDescriptor is used in the usual way (as a data descriptor
    instantiated during class definition).

    At least for now, *name* is required to be ``_dtype``.

    *attr_name* is derived from *name* at access time. For now, it is always
    ``__dtype``.

    Instances of the Python class may have their own *dtype*. For the SCALE-MS
    data model, TypeIdentifier is an instance attribute rather than a class attribute.
    If an instance did not set ``self.__dtype`` at initialization, the descriptor
    returns *base* for the instance's class.

    *base* is the (default) SCALEMS TypeIdentifier for the class using the descriptor.
    For a class using the data descriptor, *base* is inferred from the class
    __module__ and __qualname__ attributes, if not provided through the class definition.

    A single data descriptor instance is used for a class hierarchy to encapsulate
    the meta-programming for UnboundObject classes without invoking Python metaclass
    arcana (so far). At module import, a TypeDataDescriptor is instantiated for
    BasicSerializable._dtype. The data descriptor instance keeps a weakref.WeakKeyDict
    mapping type objects (classes) to the TypeDataDescriptor details for classes
    other than BasicSerializable. (BasicSerializable._dtype always produces
    ``TypeIdentifier(('scalems', 'BasicSerializable'))``.)
    The mapping is updated whenever BasicSerializable is subclassed.
    """

    @property
    def attr_name(self):
        return '_owner' + self.name

    def __init__(self, name: str = None, base_type: TypeIdentifier = None):
        # Note that the descriptor instance is not fully initialized until it is
        # further processed during the creation of the owning class.
        self.name = name
        if base_type is not None:
            self._original_owner_type = TypeIdentifier.copy_from(base_type)
        else:
            self._original_owner_type = None
        self.base = weakref.WeakKeyDictionary()

    def __set_name__(self, owner, name):
        # Called by type.__new__ during class creation to allow customization.
        # Let's start with strict naming requirements for early implementations,
        # and explicitly forbid multiple instances of this data descriptor implementation
        # in the same class.
        # Note that __set_name__ is only called at most once, by type.__new__
        # for a class definition in which the descriptor is instantiated.
        # In other words, __set_name__ is called for the base class, only, and
        # __init_subclass__ is called for derived classes, only.
        if name != '_dtype':
            raise ProtocolError('TypeDataDescriptor has a strict naming protocol. Only use for a `_dtype` attribute.')
        self.name = name
        if hasattr(owner, self.attr_name):
            raise ProtocolError(
                f'No storage for data descriptor. {repr(owner)} already has an attribute named {self.attr_name}.')

        assert owner not in self.base
        assert len(self.base) == 0
        logger.debug(f'Initializing base class {owner} ownership of TypeDataDescriptor.')
        self._original_owner = weakref.ref(owner)
        if self._original_owner_type is None:
            self._original_owner_type = TypeIdentifier.copy_from(
                [str(owner.__module__)] + owner.__qualname__.split('.'))
        self.base[owner] = TypeIdentifier.copy_from(self._original_owner_type)

    # We're redoing this.
    # New rules for subclassing:
    # A subclass with the same dtype is appropriate is limited cases as a helper to customize the creation
    # of certain cases of instances of a defined WorkflowItem type.
    # Obviously, the decoding should not be overridden.
    # Custom encoding may be appropriate.
    # Let's avoid subclassing to inherit implementation details,
    # but assume it may happen. Subclasses that change the dtype must register encoder/decoder.
    # Our opportunity to check this is during the __init_subclass__ call of the class that
    # originally included the descriptor in its definition.
    # If we allow dtype to be set per instance, then we ought to allow/verify encoder/decoder
    # registration during instance creation.
    # TODO: What is the canonical place for the dtype registry?
    # Do we need a single registry? Use cases:
    #  * decoder needs a mapping of TypeIdentifiers to factories.
    #  * instances can provide an encoder, but the encoded type should be checked for decodability.
    #  * ItemViews should be checkable for completeness of input and output types.
    #  * Command classes should have implementations.
    # Tentative answer: the Decoder needs to be queriable for the availability of a decoder.
    #
    # It seems like we need to have hooks for validation or requirements
    # whether or not an Accessor descriptor is replaced in a subclass.
    def __get__(self, instance, owner) -> typing.Union['TypeDataDescriptor', TypeIdentifier]:
        # Note that instance==None when called through the *owner* (as a class attribute).
        if instance is None:
            if owner is self._original_owner():
                return self
            return self.base[owner]
        return getattr(instance, self.attr_name, self.base[owner])


# Consider a set of functions that can be used as either a recipe in __init_subclass__
# or as a stack of decorators.
# @workflow.data
# class Foo:
#     ...
#
# @restartable
# @reproducible
# @workflow.command
# class Compute:
#     ...
#
# `data` and `command` can be just short-hand for separate decorator stacks.
# E.g.
#
# @accepts(...)
# @produces(...)
# @checkpoint(...)
# @completion(...)
# @item(identifier=...)
# @encode(...)
# @decode(...)
# class ...
#
# If actually implemented as above, we could initially use descriptors as very simple annotations.
# Then we could evolve the descriptors to reduce the need for decorators, without invalidating the original decorators.


# TODO: Add some regular gc tests to catch cyclic references during development.


C = typing.TypeVar('C')


def _basic_encode(obj: Encodable) -> EncodedItem:
    try:
        data = getattr(obj, 'data')
        # TODO: Inspect more rigorously. Pre-process data member (descriptor) with decorator or init_subclass.
        if hasattr(data, 'encode'):
            data = data.encode()
        representation = EncodedItem(
            label=obj.label(),
            identity=obj.identity().encode(),
            type=obj.dtype().encode(),
            shape=tuple(obj.shape()),
            data=getattr(obj, 'data')
        )
        return representation
    except (AttributeError, TypeError) as e:
        raise APIError('Not an encodable object.') from e


# Follow the example of dataclasses.dataclass. Use a single descriptor
# and non-member functions for added functionality (e.g. don't need a member `encode`).
class Encodable(typing.Protocol):
    """Base interface for encodables.

    WorkflowItem Views and unbound Workflow items should support this interface.
    It is a bit invasive for core implementation classes, though.
    TODO: Provide a wrapper and/or decorator (like dataclasses.dataclass) that
          adds hidden members to allow implementation of an `encode` module function
          in terms of registered / non-public aspects of the wrapped class.

    Note that the Protocol does not require bound methods to be satisfied.
    It is sufficient to define an implementing class with members that are
    statically inferrable as Callable (with the indicated signatures).

    Suggestion:
        Define implementing classes in the dataclasses.dataclass style,
        assigning the result of a function call to the class member.

        Note the semantics of `__objclass__`, which could be important for
        hinting the effective
        `originator <https://github.com/python/cpython/blob/799f8489d418b7f9207d333eac38214931bd7dcc/Lib/inspect.py#L387>`__
        of the (effective) attribute (such as when binding a method defined in a non-trivial way).

    Design resolution:
        Avoid relying on annotations at run time, or even during class customization.
        It seems un-Pythonic and potentially confusing, and it seems to make
        static type checking harder to get right. However, we may need some type
        annotation to _help_ the static type checking, in some cases, but let's
        see if we can get by with inferred typing, or transitive typing through
        the return values of the helpers/descriptors.

    Differences from dataclasses.dataclass:
        Conversely to dataclasses.dataclass, we probably want the result of
        processing to give a workflow item class that is normalized to use SCALEMS pseudo-types
        or data descriptor members, rather than normalizing to the most basic type.
        E.g. dataclasses.dataclass replaces the class member value with the specified
        default value, removing the Field object after processing. This likely means
        that we should think of a decorated ScaleMS class as defining two classes:
        the raw data class and the ItemView class or workflow proxy class.

        We assume developers will write standard implementations in terms of the
        raw dataclass, but we need to allow for writing in terms of the ScaleMS intrinsics,
        such as to allow for ensemble optimizations or other special handling.

    Why getter methods instead of properties?:
        None of these data are intended to be mutable on instances, and the interface
        is conveyed by the class. Properties can implement this easily, and require
        slightly less code when using Descriptors. However, we provide somewhat clearer
        semantics by asserting accessor methods from the initial design.
        Importantly, callables allow for more powerful static type checking through typing.Protocol.
        The layer of indirection also affords slightly more flexible polymorphism during class (re)definition.

        This author also believes that accessor methods tend to have clearer implied
        semantics in the relationship between the reference obtained and the reference
        used for access. Namely, the methods in this interface should return objects
        that are not coupled to the Encodable.
        Though avoidable in a modern Data Descriptor implementation,
        it is common for a reference to a property or data member to imply a
        reference to the parent object that may keep the Encodable object alive longer than desired.
    """
    @abc.abstractmethod
    def label(self) -> typing.Union[str, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def identity(self) -> Identifier:
        raise NotImplementedError

    @abc.abstractmethod
    def dtype(self) -> TypeIdentifier:
        raise NotImplementedError

    @abc.abstractmethod
    def shape(self) -> Shape:
        raise NotImplementedError

    # TODO: Refine the requirements for the data proxy.
    @abc.abstractmethod
    def data(self) -> BaseEncodable:
        raise NotImplementedError


def make_encodable(cls: typing.Type[C], func: typing.Callable[[C], BaseEncodable] = None) -> Encodable:
    """Associate a class with an encoding schema.

    Allow validation of a compatible class definition.
    Customize duck-typing / compatibility checking (virtual subclass).
    Optionally provide custom encoding logic.
    """
    ...


EncodedT = typing.TypeVar('EncodedT', bound=Encodable)


class Decodable(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def decode(cls: typing.Type[EncodedT], encoded: EncodedItem) -> EncodedT:
        ...


def make_decodable():
    """Associate a class with an object schema and optional decoding logic."""


def set_identifier():
    """Specify an identifier scheme for class instances."""


def fingerprint():
    """Specify the attributes used for fingerprinting.

    Optionally provide additional logic for generated hashes / checksums for attributes.

    TODO: Describe suitable unit tests for developers to confirm correct equivalence and non-equivalence tests.
    """


def register_inputs():
    """Declare the input signature for the workflow item, allowing additional SCALEMS annotations.

    We could register a single input type (or protocol/meta-type) with a positional argument,
    or a set of required annotated fields. In the latter case, we would want to generate a
    class definition, so we could generate an abstract base class with `isinstance` support,
    and use it to support either compatible type positional argument inputs _or_ compatible keyword argument lists.
    """
    ...


def register_outputs():
    ...


def register_state():
    """Annotate the internal state data.

    Internal state data may be checkpointed or fingerprinted independently of
    input and output data.
    """
    ...


# In addition to the following two helpers, we can demonstrate that a script-writer
# could handle their own caching to some degree by maintaining their own metadata.
# TODO: Example code to check for the existence of a user metadata file, checking for
#  a key value pair, and either reading a task from a found UUID or writing the UUID for
#  a new created item for rediscovery on future runs.


def reproducible():
    """Describe the reproducibility of the workflow item and the parameters contributing to (non)uniqueness.

    Determine what is relevant for fingerprinting the workflow item.
    """
    ...


def cache(item, **kwargs):
    """Mark an item as cacheable, subject to the constraints described by the arguments.

    This is somewhat, but not completely orthogonal to the reproducibility...
    """
    ...


# A SCALE-MS "Serializable Type".
# TODO: use a Protocol or other constraint.
ST = typing.TypeVar('ST')


ValueT = typing.TypeVar('ValueT')

AccessorT = typing.TypeVar('AccessorT', bound='ItemAccessor')

# TODO: Can we make this Protocol rather than ABC and provide better static type checking.
class ItemAccessor(abc.ABC, typing.Generic[ValueT]):
    """Data Descriptor base class for workflow item access methods.

    Accessor methods can be established in class definitions by assigning attributes
    with instances of ItemAccessor subclasses.
    This base class provides polymorphic utility and a consistent interface
    for several basic accessors used in core interfaces.

    Workflow item interface specifications prescribe member functions.
    The specification may be enforced and implemented in terms of Data Descriptors
    derived from this base class.

    Provide hooks for validation, copying, encoding, and decoding.

    Subclassing:
        Subclasses must provide a *check_value* member function to validate managed data at run time.
        Subclasses must implement the *encode* and *decode* member functions.

    Additional Customization:
        Subclasses may define the *allowed_names* class variable with a ``set`` of allowed names for Descriptor instances.
        Alternatively, subclasses may override the *check_name* method.

        Returned objects should neither extend the life of the parent object or allow mutation of the parent object.
        If stored values need to be copied or otherwise processed, subclasses should provide a *copy* function as
        a static member function or callable instance attribute.

    Note that bound methods are ephemeral objects created during attribute access.
    Ref https://docs.python.org/3/howto/descriptor.html#functions-and-methods
    Note also that bound methods are created when attribute resolution has occurred
    through the class definition. Callable instance attributes are not converted to bound methods.

    TODO: We will likely have workflow items that are not initially fully defined. We may need to allow internal updates to, say, *shape*, or allow for lazy initialization.
    """
    # TODO: Flag to allow instance versus class values.

    allowed_names: typing.ClassVar[typing.Set[str]]
    _get: typing.ClassVar[typing.Callable[[object], ValueT]]

    attr_name: str
    private_name: str
    copy: typing.Callable[[ValueT], ValueT]
    _encoder: typing.Callable[[object, ValueT], BaseEncodable]
    _decoder: typing.Callable[[object, BaseDecoded], ValueT]

    def __set_name__(self, owner, name):
        """Use the class customization hook in the Descriptor protocol.

        Subclasses overriding __set_name__ should call super().__set_name__(owner, name)
        before returning.
        """
        self.check_name(name)
        self.attr_name = str(name)
        self.private_name = f'_{self.attr_name}'
        if hasattr(owner, self.private_name):
            raise APIError('Private data for descriptor is already assigned.')
        # Descriptor classes derived from this one could set attributes on `self`
        # to be post-processed during __set_name__, but we might as well just let
        # subclasses override __set_name__.
        # TODO: Register the descriptor for this attribute with the encoder/decoder for the owning class.
        #  We could use a decorator or the __init_subclass__ methods of the protocol classes.

    def __set__(self, instance, value):
        """Implement the Data Descriptor protocol.

        By default, ItemAccessors are not overwritten with instance values.
        Subclasses may provide different semantics by overriding this method.
        """
        raise TypeError('Method is not assignable.')

    @typing.overload
    def __get__(self: AccessorT, instance: None, owner) -> AccessorT: ...

    def __get__(self: AccessorT, instance, owner) -> typing.Union[AccessorT, typing.Callable[[object], ValueT]]:
        """Return the bound or unbound accessor method, depending on attribute resolution."""
        if instance is None:
            return self
        else:
            return types.MethodType(self.__call__, instance)

    def _get(self, instance) -> ValueT:
        value = getattr(instance, self.private_name)
        if hasattr(self, 'copy'):
            value = self.copy(value)
        return value

    def __call__(self, instance) -> typing.Optional[ValueT]:
        """Implement the unbound method.

        Required to satisfy Encodable Protocol.
        """
        # Subclasses may provide the default getter when accessed through the class binding,
        # such as before initialization (e.g. LabelAccessor).
        # May be overloaded by subclasses to take arbitrary arguments,
        # but the no-argument bound method signature is required.
        return self._get(instance)

    def set(self, instance, value):
        """Initialize the Descriptor state for an instance of the owning class.

        Classes using this Descriptor are assumed to call *set()* through their class
        attribute during __init__().
        """
        self.check_value(value)
        setattr(instance, self.private_name, value)

    @classmethod
    def check_name(cls, name: str):
        """Validate the attribute name (optional).

        Subclasses may define *allowed_names* with a set of allowable attribute names
        or override this method with an appropriate validator to use during
        the __set_name__() call during class creation.
        """
        if hasattr(cls, 'allowed_names'):
            if name not in cls.allowed_names:
                raise ValueError('Invalid attribute name for this descriptor.')

    @abc.abstractmethod
    def check_value(self, value: ValueT):
        """
        Raises:
            ValueError if validation fails.

        Subclasses should check the provided value and
            raise ValueError(f'{self!r} cannot be assigned a value of {value!r}.')
        if and only if the value is unusable.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self, instance) -> BaseEncodable:
        """Encode instance data to a Python object ready for serialization.

        Note that the Descriptor needs to be accessed through the class attribute
        to use this method."""
        value: ValueT = getattr(instance, self.private_name)
        if hasattr(self, '_encoder'):
            value = self._encoder(instance, value)
        return value

    @abc.abstractmethod
    def decode(self, deserialized: BaseDecoded) -> ValueT:
        """Decode a Python object to the value type."""
        return deserialized


class LabelAccessor(ItemAccessor[str]):
    """Manage the *label* accessor.

    Allow the *label* to be set at most one time during an object's life.
    """
    allowed_names = {'label'}

    @classmethod
    def check_value(cls, value: str):
        if isinstance(value, str):
            return
        super().check_value(value)

    def decode(self, deserialized: str):
        """Return the deserialized label string.

        Label strings require no further processing after deserialization.
        """
        self.check_value(deserialized)
        return deserialized

    def encode(self, instance) -> str:
        return instance.label()


class IdentityAccessor(ItemAccessor[Identifier], typing.Generic[IdentifierT]):
    allowed_names = {'identity'}
    identifier_type: IdentifierT

    def __init__(self: IdentityAccessor[IdentifierT], id_type: typing.Type[IdentifierT] = EphemeralIdentifier):
        self.identifier_type = id_type

    def encode(self, instance) -> BaseEncodable:
        return getattr(instance, self.private_name).encode()

    def decode(self, deserialized: BaseDecoded) -> IdentifierT:
        return self.identifier_type.copy_from(deserialized)

    def check_value(self, value: IdentifierT):
        try:
            self.identifier_type.copy_from(value)
        except Exception as e:
            raise ValueError(f'Invalid representation of {self.identifier_type!r}') from e


class DataTypeAccessor(ItemAccessor[TypeIdentifier]):
    allowed_names = {'dtype'}

    def __init__(self, default: TypeIdentifier = None):
        """Initialize the Descriptor for the owning class.

        Args:
            default: Optional base dtype for instances.

        SCALE-MS DataType is an instance value. Class implementations may constrain
        instance variance, subclass variance, and/or provide a base type, at the author's discretion.
        """
        self.default = default

    def __set_name__(self, owner, name):
        # It is not clear whether we want to implement these accessors in terms of private
        # data descriptors, or even whether the accessors should be replaced with public
        # data descriptors. We'll have to revisit this.
        # TODO: Merge with TypeDataDescriptor, or at least reconsider initialization and separation of responsibilities.
        super().__set_name__(owner, name)
        if hasattr(owner, '_dtype'):
            raise APIError('Cannot assign a dtype descriptor when a _dtype attribute already exists.')
        if self.default is not None:
            descriptor = TypeDataDescriptor(base_type=TypeIdentifier.copy_from(self.default))
            descriptor.__set_name__(owner, '_dtype')
            owner._dtype = descriptor

    def set(self, instance, value):
        # We're redoing this...
        self.check_value(value)
        # setattr(instance, self.private_name, value)


    def check_value(self, value: TypeIdentifier):
        # TODO: Check that the data type is properly registered.
        try:
            TypeIdentifier.copy_from(value)
        except Exception as e:
            raise ValueError(f'Could not extract a TypeIdentifier from {value!r}.') from e

    def encode(self, instance) -> BaseEncodable:
        """Encode the value of the dtype attribute."""
        return typing.cast(TypeIdentifier, getattr(instance, self.private_name)).encode()

    def decode(self, deserialized: BaseDecoded) -> TypeIdentifier:
        # Note: we could provide additional checking for constraints on instance dtype, but we don't yet.
        return TypeIdentifier.copy_from(deserialized)


@typing.overload
def identity_proxy() -> IdentityAccessor[EphemeralIdentifier]: ...

@typing.overload
def identity_proxy(factory: typing.Callable[[...], IdentifierT]) -> IdentityAccessor[IdentifierT]: ...

def identity_proxy(factory: typing.Callable[[...], IdentifierT] = EphemeralIdentifier) -> IdentityAccessor[IdentifierT]:
    proxy: IdentityAccessor[IdentifierT] = IdentityAccessor[IdentifierT]()
    proxy.factory = factory
    return proxy


def label_proxy():
    """Get a Data Descriptor to satisfy the *label()* requirement of Encodable."""
    return LabelAccessor()


def dtype_proxy(default=None) -> DataTypeAccessor:
    """Return a function that produces a copy of the current TypeIdentifier.

    The returned function is suitable for use as a bound method,
    per the Encodable Protocol.
    """
    identifier: TypeIdentifier
    # This logic is incomplete. *default* may not be required when we have
    # the chance to post-process a descriptor through __set_name__ hook.
    if default is None:
        # The Data Descriptor requires additional handling before it can be used.

    if default is not None:
        identifier = TypeIdentifier.copy_from(default)

    # This is simpler, but possibly less readable during introspection,
    # than a separate getter and Data Descriptor, such that the owning class
    # retains ownership of the static identifier source data, rather than this closure.
    # TODO: convert to stateless pair of Descriptors.
    def proxy(encodable: Encodable) -> TypeIdentifier:
        dtype = TypeIdentifier.copy_from(identifier)
        return dtype

    return proxy


class BasicSerializable(Encodable, Decoded):
    """
    This is the basic implementation we expect for objects that are being
    serialized or deserialized.

    Note that it is potentially too easy to overwrite attributes on instances.
    Hopefully this is not a problem. If it becomes a point of confusion or error,
    we can use Data Descriptors for all attributes to block regular attribute assignment.
    If this is still not sufficient, we can use the `__slots__` mechanism for even
    greater immutability. However, note that derivation from this class is not required
    or assumed. This is only a reference implementation of the base class Protocols,
    and implementers of workflow items are free to pursue other schemes.
    """
    # Suggestion: If we can maintain the static type hinting,
    # normalize on a single dispatching helper function (i.e. look more like dataclasses.dataclass)
    identity = identity_proxy()
    label = label_proxy()
    dtype = dtype_proxy(default=TypeIdentifier(('scalems', 'BasicSerializable')))
    shape = shape_proxy()
    data = data_proxy()

    def __init__(self, data, *, dtype, shape=(1,), label=None, identity=None):
        # TODO: These initializations can and should be automated for uniformity.
        # TODO: Check whether an attribute is "optional". For optional attributes, only call *set* if a non-None value is provided.
        self.__class__.identity.set(self, identity)
        if label is not None:
            self.__class__.label.set(self, label)

        self.__class__.dtype.set(self, dtype)
        attrname = BasicSerializable._dtype.attr_name
        setattr(self, attrname, TypeIdentifier.copy_from(dtype))

        self._shape = Shape(shape)
        # TODO: validate data dtype and shape.
        # TODO: Ensure that we retain a reference to read-only data.
        # TODO: Allow a secondary localized / optimized / implementation-specific version of data.
        self.data = data

    # TODO: encode() and decode() probably don't make sense to standardize as member functions.
    #  Note the module functions in dataclasses for comparison.
    # TODO: Make sure that BasicSerializable gets registered.
    def encode(self) -> dict:
        # TODO: allow custom encoder for data member.
        return super().encode()

    @classmethod
    def decode(cls: typing.Type[ST], encoded: dict) -> ST:
        if not isinstance(encoded, collections.abc.Mapping) or not 'type' in encoded:
            raise TypeError('Expected a dictionary with a *type* specification for decoding.')
        dtype = TypeIdentifier.copy_from(encoded['type'])
        label = encoded.get('label', None)
        identity = encoded.get('identity')  # TODO: verify and use type schema to decode.
        shape = Shape(encoded['shape'])
        data = encoded['data']  # TODO: use type schema / self._data_decoder to decode.
        logger.debug('Decoding {identity} as BasicSerializable.')
        return cls(label=label,
                   identity=identity,
                   dtype=dtype,
                   shape=shape,
                   data=data
                   )

    def __init_subclass__(cls, **kwargs):
        assert cls is not BasicSerializable

        # Handle SCALE-MS Type registration.
        base = kwargs.pop('base_type', None)
        if base is not None:
            typeid = TypeIdentifier.copy_from(base)
        else:
            typeid = [str(cls.__module__)] + cls.__qualname__.split('.')
        registry = BasicSerializable._dtype.base
        if cls in registry and registry[cls] is not None:
            # This may be a customization or extension point in the future, but not today...
            raise ProtocolError('Subclassing BasicSerializable for a Type that is already registered.')
        BasicSerializable._dtype.base[cls] = typeid

        # Register encoder for all subclasses. Register the default encoder if not overridden.
        # Note: This does not allow us to retain the identity of *cls* for when we call the helpers.
        # We may require such information for encoder functions to know why they are being called.
        encoder = getattr(cls, 'encode', BasicSerializable.encode)
        encode.register(dtype=cls, handler=encoder)

        # Optionally, register a new decoder.
        # If no decoder is provided, use the basic decoder.
        if hasattr(cls, 'decode') and callable(cls.decode):
            _decoder = weakref.WeakMethod(cls.decode)

            # Note that we do not require that the decoded object is actually
            # an instance of cls.

            def _decode(encoded: dict):
                decoder = _decoder()
                if decoder is None:
                    raise ProtocolError('Decoding a type that has already been de-registered.')
                return decoder(encoded)

            decode.register(typeid=cls._dtype, handler=_decode)

        # TODO: Register optional instance initializer / input processor.
        # Allow instances to be created with something other than a single-argument
        # of the registered Input type.

        # TODO: Register/generate UI helper.
        # From the user's perspective, an importable module function interacts
        # with the WorkflowManager to add workflow items and return a handle.
        # Do we want to somehow generate an entry-point command

        # TODO: Register result dispatcher(s).
        # An AbstractDataSource must register a dispatcher to an implementation
        # that produces a ConcreteDataSource that provides the registered Result type.
        # A ConcreteDataSource must provide support for checksum calculation and verification.
        # Optionally, ConcreteDataSource may provide facilities to convert to/from
        # native Python objects or other types (such as .npz files).

        # Proceed dispatching along the MRO, per documented Python data model.
        super().__init_subclass__(**kwargs)


def compact_json(obj) -> str:
    """Produce the compact JSON string for the encodable object."""
    # Use the extensible Encoder from the serialization module, but apply some output formatting.
    string = json.dumps(obj,
                        default=encode,
                        ensure_ascii=True,
                        separators=(',', ':'),
                        sort_keys=True
                        )
    return string


def fingerprint(obj: BasicSerializable) -> bytes:
    """Get the fingerprint for a SCALEMS object.

    Calculate and return the checksum for the normative raw form of the object.

    By default, this is the compact JSON serialization of the objects *encode* output.
    Registered classes may override this default calculation.
    """
    encoded = encode(obj)
    string = compact_json({key: value for key, value in encoded.items() if key not in ('label', 'identity')})
    checksum = hashlib.sha256(string).digest()
    return checksum


class JsonObjectPairsDispatcher:
    """Decode key/value pairs from JSON objects into SCALE-MS objects.

    Provides a place to register different type handlers.

    Each JSON *object* deserialized by the JSON Decoder is passed as a sequence
    of (key, value) pairs. The result is returned instead of the usual *dict(pairs)*.

    We don't have interaction with state or nesting, so we may have to examine the
    values for Python objects that have already been decoded to see if additional
    processing is necessary once the context of the key/value pair is known.
    """

    def __call__(self, key, value):
        ...


# def object_pair_decoder(context, object_pairs: typing.Iterable[typing.Tuple[str, typing.Any]]) -> typing.Iterable[ItemView]:
#     """Decode named objects, updating the managed workflow as appropriate.
#
#     For object pairs representing complete workflow items, get a handle to a managed workflow item.
#     If the key is already managed, update the the managed item or raise an error if the managed item
#     is not consistent with the received item.
#
#     Note that responsibilities for validating work graphs, data flow, and compatibility are delegated to the
#     WorkflowManager and the registered data and command types. It does not make sense to call this function without
#     a proper WorkflowManager. For low-level testing or other use cases, consider directly using PythonDecoder.
#
#     To extend json.load() or json.loads(), use functools.partial to bind a workflow context, and pass the
#     partially bound function as the *object_pairs_hook* argument to the json deserializer.
#     """
#     # We would generally want to deserialize directly into a WorkflowManager. We could write this as a free function
#     # and optionally bind it as a method. We could also make it a singledispatch function or a singledispatchmethod.
#     # These are probably not mutually exclusive.
#     for key, value in object_pairs:
#         # dispatch decoding for value
#         # if is_workflowitem(decoded):
#         #    identity = validate_id(key, decoded)
#         #    record = {identity: decoded}
#         item_view = context.add_item(record)
#         yield item_view

Key = typing.Union[str, int, slice]


class OperationIdentifier(tuple):
    """Python structure to identify an API Operation implementation.

    Operations are identified with a nested scope. The OperationIdentifier
    is a sequence of identifiers such that the operation_name() is the final
    element, and the preceding subsequence comprises the namespace().

    Conventional string representation of the entire identifier uses a period
    (``.``) delimiter.
    """

    def namespace(self):
        return tuple(self[0:-2])

    def operation_name(self):
        return self[-1]

    def __str__(self):
        return '.'.join(self)


class OperationNode(abc.ABC):
    """Abstract interface for operation node references."""

    @abc.abstractmethod
    def to_json(self, **json_args) -> str:
        """Serialize the node to a JSON record."""
        ...

    @classmethod
    def from_json(cls, serialized: str):
        """Creation method to deserialize a JSON record."""
        # TODO: We could, of course, dispatch to registered subclasses,
        #  but this is deferred until refactoring converts node references into
        #  views into a Context.
        ...

    @abc.abstractmethod
    def fingerprint(self):
        """Get the unique identifying information for the node."""
        ...
