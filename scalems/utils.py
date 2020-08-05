"""Utility functions and decorators to simplify ScaleMS compliance."""


def command(*, input_type, result_type):
    """Get a decorator for ScaleMS Command definitions.

    A ScaleMS command minimally consists of an input specification, and output
    specification, and a callable.
    """
    def decorator(cls):
        ...
    return decorator
