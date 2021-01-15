"""Support unique identification of resources and data flow.

The fingerprint data is the 32 bytes from a sha256 hash digest.

The hash is performed on the utf-8 encoded compact JSON string representation
of the unique identifying information of the object being fingerprinted.

Objects have several possible representations.

The fundamental representation is the JSON-encoded string representation.

The next most fundamental representation is a default-encodable Python object (TODO).

In both of these first two cases, we initially hash the entire object representation,
but first normalize to a compact formatting with sorted keys.
TODO: Hash only identifying information, according to SCALE-MS specification.

Higher level representations are objects supporting the SCALE-MS specified
SupportsFingerprint Protocol (TODO).

Calling code may use the support.serialization module to achieve a basic JSON encoding
(or encodable object) for processing within this module.
"""
from __future__ import annotations

__all__ = ['Fingerprint']

# The behavior of `bytes` is sufficient that a UID class is probably not necessary,
# though we might want to guarantee that a UID is exactly 32 bytes. TBD...
import abc
import collections
import collections.abc
import functools
import hashlib
import json
import os
import typing
import uuid
import warnings

from scalems.core.exceptions import InternalError

@functools.singledispatch
def _hash(obj) -> bytes:
    """Provide a dispatching function for normalized hashing of raw data."""
    # Warning: Since we only hash the data, we do not retain type identity.
    raise TypeError('No dispatcher to hash {}'.format(repr(obj)))


@_hash.register(float)
@_hash.register(int)
@_hash.register(str)
@_hash.register(bool)
@_hash.register(type(None))
def _(obj) -> bytes:
    # Do we need to handle bare data?
    return hashlib.sha256(compact_json(obj).encode('utf-8')).digest()


@_hash.register(list)
@_hash.register(tuple)
def _(obj: typing.Sequence) -> bytes:
    # This should probably be further dispatched in terms of the appropriate
    # network binary packing, such as XDR.
    import numpy
    h = hashlib.new('sha256')
    if len(obj) > 0:
        h.update(numpy.array(obj))
    return h.digest()


@_hash.register(dict)
def _(obj: typing.Mapping[str, BaseEncodable]) -> bytes:
    # Do we have bare dicts? Or do dicts always represent objects with schema?
    h = hashlib.new('sha256')
    for key, value in obj.items():
        h.update(key.encode('utf-8'))
        # Warning: we are not checking for circular references.
        h.update(_hash(value))
    return h.digest()


def _fingerprint(obj) -> bytes:
    """Generate the SHA256 digest hash to identify an object.

    .. todo:: Handle shape correctly.
        Spam(['spam', 'spam']) should have the same fingerprint as [Spam('spam'), Spam('spam')]

    """
    # For fingerprinting:
    # 1. Acquire a valid WorkflowItem (or some prototype of a not-yet-managed item).
    # 2. Create a dict of the identifying information.
    # 3. Get the compact_json for the dict.
    # 4. Generate the sha256 hash.
    item = _get_item(obj)
    record_of_note = _filter_identifiers(item)
    normalized: str = compact_json(record_of_note)

    hashed = hashlib.sha256(normalized.encode('utf-8'))
    assert hashed.digest_size == 32
    return hashed.digest()


def _get_item(obj):
    """Get a normalized view to a representation of a workflow item."""
    # TODO: Need to process obj!
    warnings.warn('_get_item() needs implementation.')
    return obj


def _filter_identifiers(item: Encoded) -> Encoded:
    """Get the subset of an encoded item record that uniquely identifies the product.

    Node uniqueness comprises a semantic symbol and any parameters affecting the data produced.
    """
    # TODO: filter in terms of member annotations.
    return item



def calculate_fingerprint(schema, inputs) -> FingerprintHash:
    """Use schema to generate a hash of relevant fingerprint data.

    TODO: Richer schema handling.
    """
    # TODO: How to handle missing values?

    ...


@functools.singledispatch
def get_uid_string(obj) -> str:
    # Handle objects with 'identity' attributes if a more specific overload is not registered.
    identity = getattr(obj, 'identity', None)
    if isinstance(identity, str):
        return identity
    if isinstance(identity, bytes):
        return identity.hex()
    raise TypeError('Object does not implement a Fingerprinted protocol.')


@get_uid_string.register
def _(obj: collections.abc.Mapping) -> str:
    identity = obj.get('identity', None)
    if not isinstance(identity, str):
        raise TypeError('Object does not implement a Fingerprinted protocol.')
    else:
        return identity


@get_uid_string.register
def _(obj: str) -> str:
    # Handle strings that are JSON-serialized objects
    try:
        pyobj = json.loads(obj)
    except json.JSONDecodeError:
        pyobj = None
    if isinstance(pyobj, dict):
        identity = pyobj.get('identity', None)
        if isinstance(identity, str):
            return identity
    raise TypeError('Object does not implement a Fingerprinted protocol.')


def get_uid_bytes(obj) -> bytes:
    """Get the fingerprint hash digest, if the object has been fingerprinted already.

    If the object is already fingerprinted, return the fingerprint hash.

    Raises:
        TypeError if object does not appear to be a Fingerprinted type.

        ValueError if object or fingerprint violate schema.
    """
    try:
        identity = get_uid_string(obj)
        digest = bytes.fromhex(identity)
    except ValueError as e:
        raise ValueError('Schema violation.') from e
    size = len(digest)
    if not size == 32:
        raise ValueError('Expected digest_size 8, but got {}'.format(size))
    return digest


def get_fingerprint(obj) -> bytes:
    """Get the fingerprint of an object.

    If the object is already fingerprinted, return the fingerprint hash.
    Otherwise, raise TypeError.

    TODO: Additional dispatching as object representation expands.
    TODO: Additional error checking to make sure we are fingerprinting the normalized representation for the object type.
    """
    # First, check. Return an existing digest if present.
    try:
        digest = get_uid_bytes(obj)
        return digest
    except ValueError as e:
        raise ValueError('Schema violation.') from e
    except TypeError:
        ...

    # Try to calculate a digest
    if isinstance(obj, collections.abc.Mapping):
        ...
        return calculate_fingerprint(schema, inputs)

    raise ValueError('Could not get a fingerprint.')

