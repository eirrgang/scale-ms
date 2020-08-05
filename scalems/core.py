"""Core details for ScaleMS implementation."""
# TODO: Helpers and optimizations for fused operations, "partial" operations.
# TODO: Distinguishing features of "dynamic" operations that can create new instances during execution.
# TODO: Define Subgraph in terms of fused / partial operations.
# TODO: Define "while_loop" in terms of dynamics operations.


class _PrototypicalDescriptor:
    """A prototypical Descriptor class for illustration / quick reference."""

    # Ref: https://docs.python.org/3/reference/datamodel.html#implementing-descriptors
    def __set_name__(self, owner, name):
        # Called by type.__new__ during class creation to allow customization.
        ...

    def __get__(self, instance, owner):
        # Note that instance==None when called through the *owner* (as a class attribute).
        ...

    def __set__(self, instance, value):
        # If defined, the descriptor is a Data Descriptor and will not be overridden in instances.
        ...

    def __delete__(self, instance):
        # Optional method for Data Descriptors.
        ...


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
    type(name=name, bases=(), )


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
