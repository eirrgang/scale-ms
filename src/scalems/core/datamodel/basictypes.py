"""Specify some basic types used in the Python data model."""

__all__ = []


import typing

# Data dimensions are fundamentally of fixed integral size.
# Some special placeholders will evolve to allow for dimensions of unknown size,
# but the scenarios under which this is possible will need to be discussed and refined.
# DimensionSize = typing.Union[int]

class DimensionSize:
    """Type for dimension sizes in a Shape tuple.

    Fundamentally, a dimension has integral size, but some special
    values exist for special cases in which dimensions are either
    not of fixed size or the size is not known at the time of creation
    for the Shape object.
    """


class Shape(typing.Tuple):
    """Number and size of multidimensional resources."""


class N(DimensionSize):
    """Placeholder for a dimension of indefinite size.

    In the future, we will likely need several variations of this sort of placeholder.

    Consider that we may have an array object whose size is fixed, but not known
    until run time. It's size will be known before consumers need to be instantiated.
    On the other hand, we may produce an iterator and allow that some tasks do not
    need to know the number of elements until some point during execution.

    It may be important to be able to mark dimensions in several resources that
    must agree with each other, but which can't be known until run time.
    We might expect that instances of this placeholder will become data objects
    in the workflow and will acquire a fixed value at some point.
    """
    def __init__(self, min=0, max=None):
        self.min = min
        self.max = max

    def bind(self, size):
        """If a size is compatible, bind this instance to the size.

        Raises:
             DataShapeError if size is incompatible.
             ProtocolError if instance is already bound to an incompatible size.

        """


class Mapping(DimensionSize):
    """Annotate a dimension with key-value semantics.

    This is a simple data annotation for simple value types.
    More elaborate associative data protocols will probably need
    to evolve.

    Keys are string type. Value types are indicated at instantiation.
    """
    def __init__(self, value_type: type):
        self.value_type = value_type


# TODO: filesystem Path type needs to be wrapped in something with SCALE-MS shape.


