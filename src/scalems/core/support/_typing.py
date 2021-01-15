"""Protocols and static type check support.

Classes in this module are intended for checking compatibility at run time,
not (necessarily) for direct inheritance.

For type hinting and lighter weight static enforcement of specified API interfaces,
see datamodel.interfaces.

"""

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

