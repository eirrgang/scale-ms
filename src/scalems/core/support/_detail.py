"""Implement composable details.

Decorators, data descriptors, and mix-in utilities.
Abstract base classes and metaprogramming support.

The parallel _typing module provides interface specifications for
compatibility checking without providing run time implementation details.

The types here are not part of the specification and should not be exposed
in user-facing code.
"""

from __future__ import annotations

__all__ = []

import abc
import typing
import weakref
from dataclasses import dataclass

from scalems.core.exceptions import ProtocolError
from scalems.core.exceptions import ScopeError
from scalems.core.support.fingerprint import Identifier
from scalems.core.support.fingerprint import NamedIdentifier


class TypeIdentifier(NamedIdentifier):
    def name(self):
        return self._name_tuple

    @classmethod
    def copy_from(cls, typeid) -> 'TypeIdentifier':
        """Create a new TypeIdentifier instance describing the same type as the source.
        """
        if isinstance(typeid, NamedIdentifier):
            return cls(typeid._name_tuple)
        if isinstance(typeid, (list, tuple)):
            return cls(typeid)
        if isinstance(typeid, type):
            # Consider disallowing TypeIdentifiers for non-importable types.
            # (Requires testing and enforcement.)
            # Consider allowing class objects to self-report their type.
            if typeid.__module__ is not None:
                fully_qualified_name = '.'.join((typeid.__module__, typeid.__qualname__))
            else:
                fully_qualified_name = str(typeid.__qualname__)
            return cls.copy_from(fully_qualified_name)
        if isinstance(typeid, str):
            # TODO: First check if the string is a UUID or other reference form for a registered type.
            return cls.copy_from(tuple(typeid.split('.')))
        # TODO: Is there a dictionary form that we should allow?


class Integer(abc.ABC):
    """Define the core Integer type.

    Base implementation depends on numpy.
    """
    def __init__(self, *args, **kwargs):
        import numpy
        self.data = numpy.array(*args, **kwargs)


class BaseObject:
    """Base class for Scale-MS workflow objects."""
    identifier: Identifier

    def __init_subclass__(cls, **kwargs):
        """Process special attributes on derived classes.

        Help implement the SCALE-MS data model and provide automatic
        serialization/deserialization support.
        """
        super().__init_subclass__(**kwargs)
        for key, value in cls.__dict__.items():
            # TODO: prefer annotation to value type.
            if isinstance(value, ...):
                ...


    def encode(self) -> dict:
        """Convert to a serializable structure.
        """
        # TODO: Stronger type hinting. Consider PEP 544 instead of PEP 484.

    @classmethod
    def decode(cls, encoded):
        """Produce an instance of the subclass that produced *encoded*."""
        # TODO: Better type hinting.


class ResourceType(BaseObject):
    ...


class WorkflowItem(BaseObject):
    """An object that may be referenced in a workflow and participate in data flow.

    Locatable by *identity*, a unique identifier.

    TODO: the unique identifier is a Fingerprint.
    """
    def __init__(self, label: str = None):
        # TODO: Fingerprint the input and store a UID.
        self._uid = ...
        self._label = label

    # TODO: Replace with reusable Fingerprinter data descriptor instance?
    @property
    def identity(self):
        return self._uid


class DataType(ResourceType):
    ...


class CommandType(ResourceType):
    ...


class CommandInstance(WorkflowItem):
    # Note that we can't do static type checking on *input* and *result* with a
    # single CommandInstance type, so the more complete data model will probably
    # use Python class/instance semantics for CommandType and CommandInstance.
    ...


class DataInstance(WorkflowItem):
    ...


# For fields on Instance types,
# *input* and *result* are Python data descriptors (InputField and ResultField?)
# that return instances of InputReference and ResultReference, which may or may not
# have different semantics that warrant separate derivation from Reference.
#
# For fields on ResourceType types,
# *input* and *result* attributes hold Description instances.



class TaskField:
    """Data Descriptor for fields in Operation Input/Output types."""
    # Ref: https://docs.python.org/3/reference/datamodel.html#implementing-descriptors

    def __init__(self, resource_type, *, shape: tuple, doc: str = None, **kwargs):
        # Attribute name associated with this field. Will be discovered with __set_name__ during
        # creation of the class that will own this descriptor.
        self.name = None
        # Proxied attribute name for the owning class. Used during __get__ and __set__
        self.internal_name = None
        # TODO: We can support a more elaborate proxy behavior than this, if necessary.

        # TODO: What are the requirements for *resource_type*?
        # assert isinstance(resource_type, (ResourceType, ResourceTypeLabel))
        self.resource_type = resource_type

        # Note that *shape* may be constrained by whether this is an input or output field. TBD.
        if not isinstance(shape, tuple):
            raise TypeError('*shape* argument must be a tuple.')
        self.shape = shape

        # Accept additional optional annotations.
        # TODO: Defer to subclass __init__ or handle with more rigor.
        self.options = {}
        for key, value in kwargs.items():
            self.options[str(key)] = value

        if doc is not None:
            self.__doc__ = str(doc)

    def __set_name__(self, owner, name):
        # Called by type.__new__ during class creation to allow customization.
        assert isinstance(name, str)
        # Note: In the current protocol, the descriptor will be created in one class definition,
        # but used in another. It is not yet clear whether we should make note of these classes
        # for context-specific semantics.
        self.name = name
        self.internal_name = '_field_' + name
        # TODO: additional logic to check or set up resource management?

    def __get__(self, instance, owner):
        # Note that instance==None when called through the *owner* (as a class attribute).
        raise AttributeError('{}.{} is not readable.'.format(instance.__class__.__name__, self.name))

    def __set__(self, instance: object, value):
        # If defined, the descriptor is a Data Descriptor and will not be overridden in instances.
        raise AttributeError('{}.{} is not assignable.'.format(instance.__class__.__name__, self.name))


class InputField(TaskField):
    """Data Descriptor for fields in Operation Input types."""
    # Ref: https://docs.python.org/3/reference/datamodel.html#implementing-descriptors

    def __get__(self, instance, owner):
        # Note that instance==None when called through the *owner* (as a class attribute).
        if instance is None:
            return description
        else:
            # TODO: This should probably return a Reference object.
            # Note that the specification indicates inputs are fixed at instance creation time.
            return getattr(instance, self.internal_name)


class OutputField(TaskField):
    """A prototypical Descriptor class for illustration / quick reference."""

    # Ref: https://docs.python.org/3/reference/datamodel.html#implementing-descriptors

    def __get__(self, instance, owner):
        # Note that instance==None when called through the *owner* (as a class attribute).
        if instance is None:
            return description
        else:
            # TODO: Wrap returned value with Future semantics.
            # Note that the specification indicates that internal representation is in
            # the form of a mutable Reference, but user-facing syntax probably expects
            # Future semantics.
            return getattr(instance, self.internal_name)


def _make_datatype(name: str, fields):
    type(name=name, bases=())


class OperationBase:
    """Base class for operations that can be managed as ScaleMS workflow tasks.

    This may be subclassed to allow easy definition of new or overloaded operations
    with minimal boilerplate.

    Variables from the derived class namespace (Fields) will be interpreted
    to generate additional supporting types.

    For discussion of generated facets and implementation requirements,
    see tracked issue https://github.com/SCALE-MS/scale-ms/issues/14
    """
    # A small number of keyword attributes are reserved for special ScaleMS semantics.
    label: str
    # context: 'WorkflowContext'

    def __init_subclass__(cls, **kwargs):
        """Finalize the definition of an immediate subclass.

        Note that this is called _after_ type.__new__ has collected and called
        __set_name__ for descriptor objects found in the new class namespace.
        Reference https://docs.python.org/3/reference/datamodel.html#creating-the-class-object

        .. todo:: Consider replacing with a decorator.

        """
        # Process InputField descriptors and define Input DataType.
        input_fields = [attr for attr in cls.__dict__.values() if isinstance(attr, InputField)]
        assert 'InputType' not in cls.__dict__
        cls.__dict__['InputType'] = _make_datatype('InputType', input_fields)
        # Question: Do we want to leave these attributes in the class definition?

        # Process OutputField descriptors and define Output DataType.
        output_fields = [attr for attr in cls.__dict__.values() if isinstance(attr, OutputField)]
        assert 'ResultType' not in cls.__dict__
        cls.__dict__['ResultType'] = _make_datatype('ResultType', output_fields)
        # Question: Do we want to reuse the descriptor objects or replace them with other functionality?
        # Question: Where do we want to register the implementation? In the base class? With the Context?

        # Define the Operation factory.
        def factory():
            ...
        assert '_factory' not in cls.__dict__
        cls.__dict__['_factory'] = factory

        # Generate the docstring for the new Operation factory.
        # Generate the docstring for the new Input type.
        # Generate the docstring for the new Output type.


class ItemView:
    """Standard object returned by a WorkflowContext when adding details to the workflow.

    Provides the normative interface for workflow items as a user-space object
    that proxies access through a workflow manager.

    Provides a Future like interface.

    At least in the initial implementation, a ItemView does not extend the lifetime
    of the Context to which it refers. If the Context from which the ItemView was
    obtained goes out of scope or otherwise becomes invalid, some ItemView interfaces
    can raise exceptions.

    .. todo:: Allows proxied access to future results through attribute access.

    """
    def identity(self) -> bytes:
        """Get the canonical unique identifier for this task.

        The identifier is universally unique and can be used to query any
        workflow manager for awareness of the task and (if the context is aware
        of the task) to get a view of the task.

        Returns:
            256-bit binary digest as a 32 element byte sequence.
        """
        return bytes(self._uid)

    def done(self) -> bool:
        """Check the status of the task.

        Returns:
            true if the task has finished.

        """
        context = self._context()
        if context is None:
            raise ScopeError('Out of scope. Managing context no longer exists!')
        return context.item(self.identity()).done()

    def result(self):
        """Get a local object of the tasks's result type.

        .. todo:: Forces dependency resolution.

        """
        context = self._context()
        if context is None:
            raise ScopeError('Out of scope. Managing context no longer exists!')
        return context.item(self.identity()).result()

    def description(self) -> Description:
        """Get a description of the resource type."""
        context = self._context()
        if context is None:
            raise ScopeError('Out of scope. Managing context no longer exists!')
        return context.item(self.identity()).description()

    def __getattr__(self, item):
        """Proxy attribute accessor for special task members.

        If the workflow element provides the requested interface, the managing
        Context will provide appropriate access. For "result" attributes, the
        returned reference supports the Future interface.
        """
        # We don't actually want to do this check here, but this is essentially what
        # needs to happen:
        #     assert hasattr(self.description().type().result_description().type(), item)
        context = self._context()
        if context is None:
            raise ScopeError('Out of scope. Managing context no longer available!')
        task = context.item(self.identity())  # type: Task
        try:
            return getattr(task, item)
        except KeyError as e:
            raise


    def __init__(self, context, identity: bytes):
        self._context = weakref.ref(context)
        if isinstance(identity, bytes) and len(identity) == 32:
            self._uid = identity
        else:
            raise ProtocolError('identity should be a 32-byte binary digest (bytes).')



# Simple command model:
# A command accepts a CommandInput instance or a combination of CommandInput Field keywords
# and additional key words for UIHelpers. The command returns a CommandInstance.
# A CommandInstance has an associated CommandType, InputType, ResultType, and
# Fingerprint factory by which the instance can be decomposed or abstracted, and
# which are used to implement the interface. The instance acts as a Future[ResultType]
# and binds to a Future[InputType], which is converted to a InputType[RuntimeContext]
# at run time.

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
