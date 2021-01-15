"""Test the scalems package data model helpers and metaprogramming utilities.
"""
from __future__ import annotations

import json
import logging
import os
import pathlib
import uuid

import pytest
import typing
from scalems.core.exceptions import ProtocolError
from scalems.core.support.serialization import BasicSerializable
from scalems.core.support.serialization import decode
from scalems.core.support.serialization import encode
from scalems.core.support.serialization import EncodedItem
from scalems.core.support.serialization import fingerprint
from scalems.core.support.serialization import Shape
from scalems.core.support.serialization import TypeIdentifier

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

record = """{
    "version"= "scalems_workflow_1",
    "types"= {
        "scalems.SubprocessInput" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessInput"],
            "fields" = {
                "argv" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "String"],
                    "shape"= ["constraints.OneOrMore"]
                },
                "inputs" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "outputs" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "stdin" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "environment" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "resources" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                }
            }            
        },
        "scalems.SubprocessResult" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessResult"],
            "fields" = {
                "exitcode" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Integer"],
                    "shape"= [1]
                },
                "stdout" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "stderr" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "file" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                }
            }
        },
        "scalems.Subprocess" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessResult"],
            "fields" = {
                "input" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "SubprocessInput"],
                    "shape"= [1]
                },
                "result" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "SubprocessResult"],
                    "shape"= [1]
                }
            }
        },
    },
    "items"= [
        {
            "label"= "input_files",
            "identity"= "832df1a2-1f0b-4024-a4ab-4160717b8a8c",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"-i"= ["infile"]}
        },
        {
            "label"= "output_files",
            "identity"= "d19e2734-a23c-42de-88a0-3287d7ca71ac",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"-o"= ["outfile"]}
        },
        {
            "label"= "resource_spec",
            "identity"= "34dfc648-27b3-47db-b6a8-a10c9ae58f09",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"ncpus"= 8, "launch_method"= ["exec"]}
        },
        {
            "label"= "subprocess_input",
            "identity"= "26c86a70-b407-471c-85ed-c1ebfa52f592",
            "type"= ["scalems", "SubprocessInput"],
            "shape"= [1],
            "data"= {
                "args"= ["myprogram", "--num_threads", "8"],
                "inputs"= "832df1a2-1f0b-4024-a4ab-4160717b8a8c",
                "outputs"= "d19e2734-a23c-42de-88a0-3287d7ca71ac",
                "stdin"= null,
                "environment" = [{}],
                "resources" = "34dfc648-27b3-47db-b6a8-a10c9ae58f09"
            },
        },
        {
            "label"= "command",
            "identity"= "199f214c-98f5-4fdd-929e-42685f8c00d2",
            "type"= ["scalems", "Subprocess"],
            "shape"= [1],
            "data"= {
                "input"= "26c86a70-b407-471c-85ed-c1ebfa52f592",
                "result"= "199f214c-98f5-4fdd-929e-42685f8c00d2"
            }
        }

    ]
}
"""

"""How to write a Command.

We don't want to rely on argument type annotations, like we did in gmxapi.
Instead, let's do something more like dataclasses.dataclass. We'll let the
signature of __init__ (and/or maybe __call__) get generated by the __init_subclass__
or decorator.

Example::
    @command
    class MyCommand:
        # This defines the class that the implementation acts on, but all of the
        # interfaces encountered by the user or the SCALE-MS framework are generated
        # in terms of this class.
        
        input = input_field()
        # Defines inputs.
        # This should also generate an input type dataclass and proxy class / helper function
        # based on the `input` member.
        # The factory for this decorated class can be created to dispatch for either an object of
        # the input type or the arguments of it.
    
        # Define outputs.
        output = output_field()
        # It will be hard to instruct a static type checker to validate data flow
        # in terms of these decorated fields. This is not always important, but we should
        # find a reasonable strategy to address it. Developers may provide explicit
        # details of the result type, but it may be redundant with this class body.
        # Note that the appropriate way to annotate a consuming command would intuitively be
        #     def consumer(data: Future[MyCommand]):
        # or
        #     def consumer(data: Future[MyCommandResult])
        # but this might not be possible to implement.
        #
        # The gist is that the static type checker cannot generally inspect generated types or signatures.
        # (Not sure why dataclasses seem to be processed so well. It may involve special handling.
        # Python 3.9 additions with typing.Annotated may help.)
        #
        # We may be able to play tricks with the readability, compatibility, or mutability of
        # the members to allow Future[MyCommand] to appear to have the intended accessors,
        # even excluding internal-only or input members if we don't intend to expose those in the result.
        #
        # A challenge is that the implementation function for MyCommand instances needs
        # to see a different interface than consumers of MyCommand outputs, and _only_
        # the registered implementation for MyCommand should see that interface.
        #
        # For reference, we should look at how the generics typing.TypedDict and NamedTuple are implemented,
        # or, rather, how the type-checking for them is implemented. It is probably time to start reading
        # the mypy source, and including it in our unit tests to build intuition. It could also be informative
        # to look into the trajectory of effort under the Parsl project related to their "towards static type checking" issue.
        #
        # To the extent that we can't get what we want from static type-checking, we need to
        # be extra-friendly to run-time introspection.
        # I think this means we pretty much have to generate a (private) metaclass for
        # input classes and result classes so that we can implement `isinstance(data, MyCommand.result_type())`.
        # Also note that static type checkers will absolutely not be able to handle
        # deserialized objects without explicit type annotations or casting (typing.cast).
        #
        # In short: We should encourage developers to find non-type-based ways to validate or dispatch their input handling.
    
        # Define implementation (optional). This is the function that will be called to fulfill
        # a Future of this command type. The decorator returns something like a functools.singledispatchmethod
        # that can be used to register "overloads" for different workflow contexts.
        # Otherwise, the name of the decorated function does not matter to ScaleMS,
        # but only one such decorated function may exist per class.
        # (Enforcement: TBD)
        #
        # Note that we may add arguments to the signature in the future, but we should
        # try to make sure it is optional whether the implementation uses them.
        # The callable should assign the output member variables.
        #
        # To allow unbound methods to be used as implementation functions, we can
        # allow the functor to be called on an annotated function. Semantics TBD.
        #
        # In some cases, there is no need for a separate implementation function.
        # The result may be clearly composable through the ordered processing of the
        # class body to determine the results.
        @implementation
        def _(self):
            self.output = self.input
"""

# TODO: Basic element data types: Integer, Float, Bool, String, BLOB? (or "unknown" / "deferred"?)
# TODO: Basic structured data types: Dense array, Mapping
# Consider: Maybe data does not intrinsically have shape, but Workflow items intrinsically have shape.

class SubprocessInput:
    """Implement scalems.SubprocessInput.

    Data fields: args, inputs, outputs, stdin, environment, resources
    """

def test_shape():
    shape = Shape((1,))
    assert isinstance(shape, tuple)
    assert len(shape) == 1
    assert shape == (1,)

    shape = Shape((3, 3))
    assert isinstance(shape, tuple)
    assert len(shape) == 2
    assert shape == (3, 3)

    shape = Shape(Shape((1,)))
    assert isinstance(shape, tuple)
    assert len(shape) == 1
    assert shape == (1,)
    assert shape == Shape((1,))

    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        Shape(1)


def test_encoding_str():
    """Confirm that bare strings are encoded and decoded as expected.

    Note: we may choose not to support bare strings through our serialization module.
    """
    string = 'asdf'
    serialized = json.dumps(string, default=encode)
    round_trip = json.loads(serialized)
    assert string == round_trip


def test_encoding_scalars():
    """Confirm that scalar data is encoded and decoded as expected.

    Note: We may choose not to support certain forms of data for the full round trip, particularly bare scalars.
    """
    scalar = 42
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip

    scalar = None
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip

    scalar = True
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip


def test_encoding_int():
    """Confirm that integer data is encoded and decoded as expected.

    Note: We may choose not to support certain forms of integer data for the full round trip.
    """
    series = [1, 1, 2, 3, 5]
    length = len(series)
    shape = (length,)

    # Test bare native int list.
    serialized = json.dumps(series, default=encode)
    round_trip = json.loads(serialized)
    assert all([a == b for a, b in zip(series, round_trip)])

    # We may expect structured data to be automatically translated to SCALE-MS representation.
    # assert round_trip.shape() == shape
    # assert round_trip.dtype() == ('scalems', 'Integer')

    array = [[1, 1], [2, 1], [8, 9]]
    shape = (3, 2)
    serialized = json.dumps(array, default=encode)
    round_trip = json.loads(serialized)
    for rowA, rowB in zip(array, round_trip):
        assert all([a == b for a, b in zip(rowA, rowB)])
    # We expect structured data to be automatically translated to SCALE-MS representation.
    # assert round_trip.shape() == shape
    # assert round_trip.dtype() == ('scalems', 'Integer')


def test_encoding_bytes():
    length = 8
    data = (42).to_bytes(length=length, byteorder='big')
    serialized = json.dumps(data, default=encode)
    round_trip = json.loads(serialized)
    # TODO: decoder
    assert round_trip == data.hex()
    # assert data == bytes(round_trip)


def test_encoding_fileobject():
    import tempfile
    with tempfile.NamedTemporaryFile() as fh:
        filename = fh.name
        assert os.path.exists(filename)
        serialized = json.dumps(filename, default=encode)
        round_trip = json.loads(serialized)
        # TODO: decoder
        assert os.path.exists(round_trip)
        # assert round_trip.dtype() == ('scalems', 'File')
        # assert round_trip.shape() == (1,)
        assert pathlib.Path(filename) == pathlib.Path(round_trip)


def test_basic_decoding():
    # Let the basic encoder/decoder handle things that look like SCALE-MS objects.
    encoded: EncodedItem = {
        'label': None,
        'identity': uuid.uuid4().hex,
        'type': ['test', 'Spam'],
        'shape': [1],
        'data': ['spam', 'eggs', 'spam', 'spam'],
    }
    instance = decode(encoded)
    assert type(instance) is BasicSerializable
    shape_ref = Shape((1,))
    assert instance.shape() == shape_ref
    type_ref = TypeIdentifier(('test', 'Spam'))
    instance_type = instance.dtype()
    assert instance_type == type_ref

    # Test basic encoding, too.
    # TODO: Register BasicSerializable.encode().
    assert tuple(encode(instance)['data']) == tuple(encoded['data'])
    assert instance.encode() == decode(instance.encode()).encode()
    # TODO: Check non-trivial shape.


# def test_basic_decoding():
#     # Let the basic encoder/decoder handle things that look like SCALE-MS objects.
#     encoded = {
#         'label': None,
#         'type': ['test', 'Spam'],
#         'shape': [1],
#         'data': ['spam', 'eggs', 'spam', 'spam']
#     }
#     # identity = fingerprint(encoded)
#     # encoded['identity'] = identity.hex()
#     instance = decode(encoded)
#     assert type(instance) is BasicSerializable
#     shape_ref = Shape((1,))
#     assert instance.shape() == shape_ref
#     type_ref = TypeIdentifier(('test', 'Spam'))
#     instance_type = instance.dtype()
#     assert instance_type == type_ref
#
#     # Test basic encoding, too.
#     assert tuple(instance.encode()['data']) == tuple(encoded['data'])
#     assert instance.encode() == decode(instance.encode()).encode()
#
#     assert instance.identity() == fingerprint(instance)
#     # TODO: Check non-trivial shape.


def test_encoder_registration():
    # Test low-level registration details for object representation round trip.
    # There were some to-dos of things we should check...
    ...

    # Test framework for type creation and automatic registration.
    class SpamInstance(BasicSerializable, base_type=('test', 'Spam')):
        ...

    instance = SpamInstance(label='my_spam',
                            identity=uuid.uuid4().hex,
                            dtype=['test', 'Spam'],
                            shape=(1,),
                            data=['spam', 'eggs', 'spam', 'spam']
                            )
    assert not type(instance) is BasicSerializable

    encoded = encode(instance)
    decoded = decode(encoded)
    assert not type(decoded) is BasicSerializable
    assert isinstance(decoded, SpamInstance)

    del instance
    del decoded
    del SpamInstance
    import gc
    gc.collect()
    with pytest.raises(ProtocolError):
        decode(encoded)

    decode.unregister(TypeIdentifier(('test', 'Spam')))
    decoded = decode(encoded)
    assert type(decoded) is BasicSerializable


def test_serialization():
    ...


def test_deserialization():
    # Check for valid JSON
    serialized = json.dumps(record, default=encode)
    assert json.loads(serialized)

    ...


def test_fingerprint():
    ...


def test_object_model():
    """Check that the decorators and annotation types work right.

    Class definitions use decorators and members of special types to generate
    definitions that support the SCALE-MS object model.

    Independently from any static type checking support, this test confirms that
    the utilities actually produce the objects and relationships we expect.
    """
