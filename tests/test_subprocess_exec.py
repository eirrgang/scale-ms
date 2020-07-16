"""Test the scalems._subprocess.exec command."""

from scalems._subprocess import exec as sp_exec


def test_exec():
    import asyncio
    cmd = sp_exec('/bin/echo')
    asyncio.run(cmd)
