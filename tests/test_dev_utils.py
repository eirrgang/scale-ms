"""Test the utilities for authoring SCALE-MS command and data types.

Note that the the scalems.executable is actually a fairly
sophisticated command. Let's pick it apart.
"""
from __future__ import annotations

import collections.abc

import typing
from scalems.core.support._detail import TypeIdentifier

# ResourceType prototype
_subprocess_type: TypeDict = {
    'schema': {
        'name': 'ResourceType',
        'spec': 'scalems.v0'
    },
    'implementation': ['scalems', 'subprocess', 'Subprocess'],
    'fields': {
        'input': {
            'schema': {
                'name': 'InputField',
                'spec': 'scalems.v0'
            },
            'type': ['scalems', 'SubprocessInput'],
            'shape': [1]
        },
        'result': {
            'schema': {
                'name': 'ResultField',
                'spec': 'scalems.v0'
            },
            'type': ['scalems', 'SubprocessResult'],
            'shape': [1]
        }
    }
}

# DataType
_subprocess_input_type = TypeDict(
    schema=SchemaDict(
        spec='scalems.v0',
        name='DataType'
    ),
    implementation=['scalems', 'subprocess', 'SubprocessInput'],
    fields={
        'argv': FieldDict(
            schema=SchemaDict(
                name='DataField',
                spec='scalems.v0'
            ),
            type=['scalems', 'String'],
            shape=[SymbolicDimensionSize(DimensionSize='OneOrMore')]
        ),
        'inputs': {
            'schema': {
                'name': 'DataField',
                'spec': 'scalems.v0'
            },
            'type': ['scalems', 'File'],
            'shape': [{'DimensionSize': 'Any'}]
        },
        'stdin': {
            'schema': {
                'name': 'DataField',
                'spec': 'scalems.v0'
            },
            "type": ["scalems", "File"],
            "shape": [1]
        },
        'environment': {
            'schema': {
                'name': 'DataField',
                'spec': 'scalems.v0'
            },
            "type": ["scalems", "Mapping"],
            "shape": [1]
        },
        'resources': {
            'schema': {
                'name': 'DataField',
                'spec': 'scalems.v0'
            },
            "type": ["scalems", "Mapping"],
            "shape": [1]
        }
    }
)

# Note: Definitely need a *cacheable* attribute to force recalculation of some things that look the same.
# It needs to be scoped...
# Both the identifier and the references to it (dependencies) need to be clear about how concrete the
# fingerprint is and whether results should be re-used or recalculated.
# There is a distinction between tasks that are deterministic or not, as well
# as workflow logic about whether a non-deterministic stage should produce a single
# output versus a fresh output each time.
# Commands that generate workflow items need to embed hints in the items, too.
# This could be done in terms of deferring the input fingerprinting to the concrete result.
#
# For instance, `read_file(filename)` cannot be uniquely identified, since we don't
# ultimately have control over what files are being written. `read_file(scalems.File(...))`
# can be uniquely identified at some scope (session scope?).
# The data it reads can be fingerprinted with global uniqueness.
# Simulation work based on the file would know whether its task should be characterized
# in terms of the concrete file fingerprint, and whether to re-execute when the file checksum changes.
# Users will expect an optional *run_once* keyword argument or something to hint that
# non-deterministic tasks should not be re-executed if results from a previous invocation are available.


_subprocess_output_type = {
    'schema': {
        'spec': 'scalems.v0',
        'name': 'DataType'
    },
    'implementation': ['scalems', 'subprocess', 'SubprocessResult'],
    'fields': {
        'exitcode': {
            'schema': {
                'name': 'DataField',
                'spec': 'scalems.v0'
            },
            "type": ["scalems", "Integer"],
            "shape": [1]
        },
        'stdout': {
            'schema': {
                'name': 'DataField',
                'spec': 'scalems.v0'
            },
            "type": ["scalems", "File"],
            "shape": [1]
        },
        'stderr': {
            'schema': {
                'name': 'DataField',
                'spec': 'scalems.v0'
            },
            "type": ["scalems", "File"],
            "shape": [1]
        }
    }
}

_subprocess_output = {
    '000000': {
        'type': ['scalems', 'subprocess', 'SubprocessResult'],
        'data': {
            'exitcode': {
                'type': ['scalems', 'Integer'],
                'shape': [1],
                'data': [0]
            },
            'stdout': {
                'type': ['scalems', 'File'],
                'shape': [1],
                'data': ['stdout.txt']
            },
            'stderr': {
                'type': ['scalems', 'File'],
                'shape': [1],
                'data': ['stderr.txt']
            }
        }
    }
}


class DataType:
    """The roles and responsibilities of a DataType are as follows...


    """


class Shape(tuple):
    """Describe the data shape of a SCALEMS object."""
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
        super(Shape, self).__init__(es)


class Integer:
    def __init__(self, *args, **kwargs):
        import numpy
        self._data = numpy.array(*args, **kwargs)

    def shape(self) -> tuple:
        return self._data.shape

    @classmethod
    def dtype(cls):
        return TypeIdentifier.copy_from(('scalems', 'Integer'))

    def encode(self):
        return {'type': self.dtype(),
                'shape': self.shape(), # warning: needs update for symbolic shape elements.
                'data': self._data.tolist()
                }

    @classmethod
    def decode(cls, encoded: dict) -> 'Integer':
        try:
            # TODO: Defer schema check to base decoder.
            if not isinstance(encoded, collections.abc.Mapping) or not all(key in encoded for key in ('type', 'shape', 'data')):
                raise TypeError('Unrecognized object received for decoding.')
            data = encoded['data']
            dtype = TypeIdentifier.copy_from(encoded['type'])
            shape = Shape(encoded['shape'])
        except TypeError as e:
            raise TypeError(f'Not a decodable object: {repr(encoded)}') from e
        if dtype != cls.dtype():
            # TODO: Consider allowing explicit transcoding.
            raise TypeError('Source is encoded for a different type.')

        return cls(data, shape=shape, dtype=int)


class Field:
    def __init__(self, dtype: DataType, shape: tuple = (1,), default=None):
        self._dtype = dtype
        self._shape = shape
        self._default = default

    def __set_name__(self, owner, name):
        # Called by type.__new__ during class creation to allow customization.
        self.name = name
        self._storage_name = '_' + name
        setattr(owner, self._storage_name, self._default)

    def __get__(self, instance, owner):
        # Note that instance==None when called through the *owner* (as a class attribute).
        if instance is None:
            return self
        else:
            return getattr(instance, self._storage_name)

    def __set__(self, instance, value):
        # If defined, the descriptor is a Data Descriptor and will not be overridden in instances.
        setattr(instance, self._storage_name, value)



class SubprocessResult:
    exitcode = Field(dtype=int, shape=(1,), default=0)
    stdout = Field(dtype=File, shape=(1,), default='stdout.txt')
    stderr = Field(dtype=File, shape=(1,), default='stderr.txt')


"""
Note on output files that don't yet exist:

We need one task type that produces an output file name to serve
as a dependency of the *argv*, if applicable. The executable result
contains a reference to the output file name from the task that
produces, but the executable result is not made ready until it
is finalized when the executable successfully completes.

For extra rigor, we can make the executable result refer to a
FileArtifact that in turn depends on the FileName result,
to allow the filename assignment to have binary/atomic state while
allowing the creation of FileArtifact to have more carefully
managed state.
"""
...


def test_fingerprint():
    _fingerprint(_subprocess_output_type)
