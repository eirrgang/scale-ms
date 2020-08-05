"""Serialization utilities."""

import abc
import collections
import json
import typing


# The behavior of `bytes` is sufficient that a UID class is probably not necessary,
# though we might want to guarantee that a UID is exactly 32 bytes. TBD...

class Fingerprint(object):
    """Convert Operation instance details to a unique identifier."""
    import hashlib as _hashlib

    def __init__(self, *,
                 operation: typing.Sequence,
                 input: typing.Union[str, typing.Mapping],
                 depends: typing.Sequence = ()):

        # TODO: replace (list, tuple) with abstraction for valid operation values
        if not isinstance(operation, (list, tuple)):
            raise ValueError('Fingerprint requires a sequence of operation name components.')
        else:
            self.operation = tuple(operation)

        # TODO: replace (dict, str) with abstraction for valid input values.
        if not isinstance(input, (dict, str)):
            raise ValueError('Fingerprint requires a valid input representation.')
        elif isinstance(input, str):
            # TODO: chase reference
            self.input = str(input)
        else:
            assert isinstance(input, dict)
            self.input = {key: value for key, value in input.items()}

        # TODO: replace (list, tuple) with abstraction for valid depends values.
        if not isinstance(depends, (list, tuple)):
            ValueError('Fingerprint requires a sequence for dependency specification.')
        else:
            self.depends = tuple(depends)

    def compact_json(self):
        identifiers = collections.OrderedDict([
            ('depends', self.depends),
            ('input', self.input),
            ('operation', self.operation)
        ])
        id_string = json.dumps(identifiers, separators=(',', ':'), sort_keys=True, ensure_ascii=True)
        return id_string

    def uid(self) -> bytes:
        """Get a 256-bit identifier.

        Returns:
            32-byte sequence.

        Conventional string formatting (text-encoded hexadecimal) is obtained
        with the ``hex()`` method of the returned value.
        """
        id_string = self.compact_json()
        id_bytes = id_string.encode('utf-8')
        id_hash = Fingerprint._hashlib.sha256(id_bytes)
        size = id_hash.digest_size
        if not size == 32:
            raise ValueError('Expected digest_size 8, but got {}'.format(size))
        digest = id_hash.digest()
        assert isinstance(digest, bytes)
        assert len(digest) == size
        return digest


def _random_uid():
    """Generate a random (invalid) UID, such as for testing."""
    import hashlib as _hashlib
    from random import randint
    return _hashlib.sha256(randint(0, 2**255).to_bytes(32, byteorder='big')).digest()


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
    def fingerprint(self) -> Fingerprint:
        """Get the unique identifying information for the node."""
        ...
