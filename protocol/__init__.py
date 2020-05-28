"""The framework for a pluggable-behavior based messaging protocol.

Messages are dispatched to the submodule specified in the first byte.
"""

import bz2
import lzma
import zlib

from abc import abstractproperty
from enum import IntEnum
from importlib import import_module
from typing import Any, Callable, Dict, Tuple

from umsgpack import ext_serializable, packb, _ext_classes

BASE_MESSAGE_CODE = 0


def identity(foo: bytes) -> bytes:
    """Return yourself, used to emulate plaintext as a comrpession type."""
    return foo


def compress_gzip(foo: bytes) -> bytes:
    """Compress using zlib with a GZIP header."""
    c = zlib.compressobj(wbits=zlib.MAX_WBITS + 16)
    return c.compress(foo) + c.flush()


def decompress_gzip(foo: bytes) -> bytes:
    """Decompress using zlib with a GZIP header."""
    return zlib.decompress(foo, wbits=zlib.MAX_WBITS + 16)


class CompressType(IntEnum):
    """Represent a compression method, including methods to apply and reverse it."""

    PLAIN = 0
    ZLIB = 1
    GZIP = 2
    LZMA = 3
    BZ2 = 4

    def compress(self, foo: bytes) -> bytes:
        """Compress some data.

        Raises
        ------
        KeyError: if the compression type is unknown (shouldn't be possible, since it's a bound method).
        """
        compressors: Dict[CompressType, Callable[[bytes], bytes]] = {
            CompressType.PLAIN: identity,
            CompressType.ZLIB: zlib.compress,
            CompressType.GZIP: compress_gzip,
            CompressType.LZMA: lzma.compress,
            CompressType.BZ2: bz2.compress
        }
        return compressors[self](foo)

    def decompress(self, foo: bytes) -> bytes:
        """Decompress some data.

        Raises
        ------
        KeyError: if the compression type is unknown (shouldn't be possible, since it's a bound method).
        """
        decompressors: Dict[CompressType, Callable[[bytes], bytes]] = {
            CompressType.PLAIN: identity,
            CompressType.ZLIB: zlib.decompress,
            CompressType.GZIP: decompress_gzip,
            CompressType.LZMA: lzma.decompress,
            CompressType.BZ2: bz2.decompress
        }
        return decompressors[self](foo)


preferred_compression = [CompressType.LZMA, CompressType.PLAIN, CompressType.ZLIB, CompressType.BZ2, CompressType.GZIP]


@ext_serializable(BASE_MESSAGE_CODE)
class BaseMessage:
    """An abstract base class for future message classes.

    Its chief job is to redirect MessagePack deserialization to the correct message protocol version.
    """

    __slots__ = {
        "protocol_ver": "The version of the message protocol currently in use",
        "message_type": "The type code for the message",
        "sender": "The sender of the message",
        "compress": "The compression method used"
    }

    def __init_subclass__(cls, **kwargs):
        """Ensure that umsgpack can recognize all the base classes for serialization."""
        _ext_classes[cls] = BASE_MESSAGE_CODE

    def __new__(cls, protocol_ver: int, *args, **kwargs):
        """Redirect construction to the correct subclass based on protocol version."""
        if cls is BaseMessage:
            module = import_module(f'.v{protocol_ver:02}', 'protocol')
            return module.Message(*args, **kwargs)  # type: ignore
        return super().__new__(cls)

    def __init__(self, protocol_ver: int, compress: int, sender: bytes = b''):
        self.protocol_ver = protocol_ver
        self.sender = sender
        self.compress = CompressType(compress)

    def __repr__(self) -> str:
        """Stub method to ensure that subclasses largely don't need to make a __repr__."""
        return f"{self.__class__.__qualname__}{self._data}"

    @abstractproperty
    def _data(self) -> Tuple[Any, ...]:
        raise NotImplementedError()  # pragma: no cover

    def packb(self) -> bytes:
        """Pack a BaseMessage as a MessagePack bytestring."""
        return bytes((self.protocol_ver, self.compress)) + self.compress.compress(packb(self._data))

    @staticmethod
    def unpackb(data: bytes) -> 'BaseMessage':
        """Given a MessagePack bytestring, reconstruct a BaseMessage."""
        protocol_ver = data[0]
        compress = CompressType(data[1])
        rest = compress.decompress(data[2:])
        module = import_module(f'.v{protocol_ver:02}', 'protocol')
        return module.Message.reconstruct(compress, rest)  # type: ignore

    @staticmethod
    def reconstruct(compress: int, data: bytes):
        """Given a compression method and decompressed data, reconstruct a BaseMessage."""
        raise NotImplementedError()  # pragma: no cover
