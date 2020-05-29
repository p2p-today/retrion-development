"""An upgrade of the naive implementation of Kademlia to provide significant improvements to the routing table."""

from concurrent.futures import Future
from copy import copy
from enum import IntEnum
from itertools import count
from time import monotonic
from typing import overload, Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, Union

from umsgpack import _ext_classes, ext_serializable, packb, unpackb

from . import preferred_compression, BaseMessage, CompressType  # noqa: F401

PROTOCOL_VERSION: int = 4

message_counter = count()


def distance(a: bytes, b: bytes) -> int:
    """Return the XOR distance between two IDs."""
    return int.from_bytes(a, 'big') ^ int.from_bytes(b, 'big')


def decide_compression(a: Sequence[int], b: Sequence[int], tie: bool) -> int:
    """Deterministically decide on a compression method according to each users' preferences.

    For each method in common, assign it a "cost" of (a.index()^2 * p) + (b.index()^2 * q) and select the method with
    the smallest "cost". p and q are chosen to be adjacent coprimes. If the ID of a is greater than the ID of b
    (indicated by the tie parameter), then q = p + 1, otherwise p = q + 1.

    Squaring the index is done to encourage the algorithm to select low index methods on *both* preference lists,
    rather than just the first. Otherwise you frequently end up with selections where you get A's favorite and B's
    least favorite method.
    """
    potential = set(a).intersection(b)  # elements in both a and b
    if potential:
        if tie:
            p, q = 6, 7
        else:
            p, q = 7, 6

        def key(x):
            a_factor = a.index(x)
            b_factor = b.index(x)
            return a_factor * a_factor * p + b_factor * b_factor * q
        return min(potential, key=key)
    return 0


class AddressType(IntEnum):
    """Enum that represents the currently supported address types."""

    UDPv4 = 0
    UDPv6 = 1


@ext_serializable(2)
class Address:
    """Base class that represents addresses in GlobalPeerInfo objects."""

    __slots__ = ('addr_type', 'args')

    def __init_subclass__(cls, **kwargs):
        """Ensure that subclasses are recognized by umsgpack."""
        _ext_classes[cls] = 2

    def __new__(cls, addr_type: AddressType, *args: Any):
        """Redirect construction to the class indicated by addr_type."""
        addr_type = AddressType(addr_type)
        if cls is Address:
            available = {
                AddressType.UDPv4: UDPv4Address,
                AddressType.UDPv6: UDPv6Address
            }
            if addr_type in available:
                return available[addr_type](addr_type, *args)
        return super().__new__(cls)

    def __init__(self, addr_type: AddressType, *args: Any):
        addr_type = AddressType(addr_type)
        self.addr_type = addr_type
        self.args = args

    def packb(self) -> bytes:
        """Pack an Address as a MessagePack bytestring."""
        return packb((self.addr_type, *self.args))

    @staticmethod
    def unpackb(data: bytes) -> 'Address':
        """Given a MessagePack bytestring, reconstruct a Message."""
        return Address(*unpackb(data))


class UDPv4Address(Address):
    """Address type that specifically represents UDPv4 addresses."""

    __slots__ = ('addr', 'port', 'addr_as_bytes')

    def __init__(self, addr_type: AddressType, addr: Union[str, bytes], port: int):
        super().__init__(addr_type, addr, port)
        if isinstance(addr, bytes):
            if len(addr) != 4:
                raise ValueError("Addresses specified as bytes must be exactly 4 bytes long")
            self.addr_as_bytes = addr
            self.addr = '.'.join(str(x) for x in addr)
        else:
            self.addr = addr
            self.addr_as_bytes = bytes(int(x) for x in addr.split('.'))
        self.port = port

    def packb(self) -> bytes:
        """Pack a UDPv4Address as a MessagePack bytestring."""
        return packb((self.addr_type, self.addr_as_bytes, self.port))


class UDPv6Address(Address):
    """Address type that specifically represents UDPv6 addresses."""

    def __init__(
        self,
        addr_type: AddressType,
        addr: str,
        port: int,
        flowinfo: Optional[int] = None,
        scopeid: Optional[int] = None
    ):
        if flowinfo is not None:
            if scopeid is not None:
                super().__init__(addr_type, addr, port, flowinfo, scopeid)
            else:
                super().__init__(addr_type, addr, port, flowinfo)
        else:
            super().__init__(addr_type, addr, port)
        self.addr = addr
        self.port = port
        self.flowinfo = flowinfo
        self.scopeid = scopeid


class PeerInfo:
    """Holds peer info, split into local and public."""

    __slots__ = {
        "local": "Information about the peer that only you need to know",
        "public": "Information about the peer that everyone knows"
    }

    def __init__(self, name: bytes, addr: Tuple[Any, ...], sock: int, your_id: bytes):
        self.local = LocalPeerInfo(sock, addr, CompressType.PLAIN, distance(name, your_id))
        self.public = GlobalPeerInfo(name, [])


@ext_serializable(1)
class GlobalPeerInfo:
    """Holds the peer info that is available to all nodes."""

    __slots__ = {
        "name": "The ID of your peer",
        "addresses": "The addresses of your peer",
        "compression": "The supported compression modes of your peer",
        "proprietary": "A dictionary that holds node info related to applications and specific implementations"
    }

    def __init__(
        self,
        name: bytes,
        addresses: List[Address],
        compression: Optional[List[CompressType]] = None,
        proprietary: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.addresses: List[Address] = addresses
        self.compression: List[CompressType] = [CompressType(x) for x in (compression or ())]
        if CompressType.PLAIN not in self.compression:
            self.compression.append(CompressType.PLAIN)
        self.proprietary = proprietary or {}

    def __repr__(self) -> str:
        """Return a string representation of the peer info."""
        return (f"GlobalPeerInfo(name={self.name!r}, addresses={self.addresses}, "
                f"compression={self.compression}, proprietary={self.proprietary})")

    def packb(self) -> bytes:
        """Pack a GlobalPeerInfo as a MessagePack bytestring."""
        return packb((self.name, self.addresses, self.compression, self.proprietary))

    @classmethod
    def unpackb(cls, data: bytes) -> 'GlobalPeerInfo':
        """Given a MessagePack bytestring, reconstruct a GlobalPeerInfo."""
        return cls(*unpackb(data))


class LocalPeerInfo:
    __slots__ = {
        "sock": "A reference to the socket this peer is connected on",
        "compression": "The compression method preferred with this peer",
        "addr": "The address of your peer",
        "distance": "The distance between you and your peer",
        "first_seen": "The timestamp when this object was created",
        "misses": "The number of messages not ACK'd within the required window",
        "hits": "The number of messages that were ACK'd or NACK'd within the required window",
    }

    def __init__(self, sock: int, addr: Tuple[Any, ...], compression: CompressType, distance: int):
        self.sock = sock
        self.addr = addr
        self.compression = compression
        self.distance = distance
        self.first_seen = monotonic()
        self.misses = self.hits = 0


class MessageType(IntEnum):
    ACK = 0
    PING = 1
    FIND_NODE = 2
    FIND_KEY = 3
    STORE_KEY = 4
    HELLO = 5
    IDENTIFY = 6
    BROADCAST = 7


class Message(BaseMessage):
    __slots__ = {
        "seq": "The sequence number of the message",
        "compress": "The compression method used"
    }

    def increment_seq(self) -> 'Message':
        """Return a copy with an increased seq field."""
        ret = copy(self)
        ret.seq = next(message_counter)
        return ret

    @staticmethod
    def reconstruct(compress: int, data: bytes):
        """Given a compression method and decompressed data, reconstruct a Message."""
        message_type, *rest = unpackb(data)
        return message_types[message_type](compress, *rest)

    def __new__(cls, compress: int = 0, message_type: int = MessageType.ACK, *args, **kwargs):
        """Make sure the Message is constructed with the correct protocol version."""
        if cls is MessageType:
            return message_types[message_type](compress, message_type, *args, **kwargs)
        else:
            return super().__new__(cls, PROTOCOL_VERSION)

    def __init__(
        self,
        compress: int = 0,
        message_type: int = MessageType.ACK,
        seq: Optional[int] = None,
        sender: bytes = b''
    ):
        super().__init__(PROTOCOL_VERSION, compress, sender)
        self.message_type = message_type
        if seq is None:
            self.seq = next(message_counter)
        else:
            self.seq = seq

    @property
    def _data(self):
        return (self.message_type, self.seq, self.sender)

    @overload
    @staticmethod
    def register(message_type: int, constructor: None = None) -> Callable[[Type['Message']], Type['Message']]:
        """Register message subclasses with a decorator."""

    @overload
    @staticmethod
    def register(message_type: int, constructor: Type['Message']) -> Type['Message']:
        """Register message subclasses with a two argument function call."""

    @staticmethod
    def register(
        message_type: int,
        constructor: Optional[Type['Message']] = None
    ) -> Union[Callable[[Type['Message']], Type['Message']], Type['Message']]:
        """Register message subclasses with a decorator or a two argument function call."""
        def foo(constructor):
            if message_type in message_types:
                raise KeyError()
            message_types[message_type] = constructor
            return constructor

        if constructor is not None:
            return foo(constructor)

        return foo

    def react(self, node, addr, sock):
        """Delay sending a PING to the sending node, since the connection is clearly active if you got a message."""
        try:
            event = node.timeouts.pop((self.sender, ))
            node.schedule.cancel(event)
        except (KeyError, ValueError):
            pass

        def ping():
            node._send(sock, addr, PingMessage())

        node.timeouts[(self.sender, )] = node.schedule.enter(60, 2, ping)

    def react_response(self, ack, node, addr, sock):
        node.logger.debug("Hey! I called react_response on a message that didn't implement it! %r", self)


message_types: Dict[int, Type[Message]] = {}


@Message.register(int(MessageType.ACK))
class AckMessage(Message):
    __slots__ = {
        "resp_seq": "The sequence number you are responding to",
        "status": "The error number of the response (0 is good)",
        "data": "Any ancillary data to go with the message"
    }

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[int] = None,
        sender: bytes = b'',
        resp_seq: int = -1,
        status: int = 0,
        data: Any = None
    ):
        super().__init__(compress, MessageType.ACK, seq, sender)
        self.resp_seq = resp_seq
        self.status = status
        self.data = data

    @property
    def _data(self):
        if self.data is None:
            if self.status == 0:
                return (*super()._data, self.resp_seq)
            return (*super()._data, self.resp_seq, self.status)
        return (*super()._data, self.resp_seq, self.status, self.data)

    def react(self, node, addr, sock):
        """Clear the message from node.awaiting_ack and call the relevant react_response() method."""
        node.logger.debug("Got an %sACK from %r (%r)", 'N' if self.status else '', addr, self)
        super().react(node, addr, sock)
        try:
            node.routing_table.member_info[self.sender].local.hits += 1
        except KeyError:
            pass
        if self.resp_seq in node.awaiting_ack:
            node.awaiting_ack[self.resp_seq].react_response(self, node, addr, sock)
            del node.awaiting_ack[self.resp_seq]


@Message.register(int(MessageType.PING))
class PingMessage(Message):
    __slots__ = ()

    def __init__(self, compress: int = 0, seq: Optional[int] = None, sender: bytes = b''):
        super().__init__(0, MessageType.PING, seq, sender)

    def react(self, node, addr, sock):
        """Since it's just a ping, we just send an ACK."""
        node.logger.debug("Got a PING from %r (%r)", addr, self)
        super().react(node, addr, sock)
        node._send(sock, addr, AckMessage(resp_seq=self.seq))


@Message.register(int(MessageType.FIND_NODE))
class FindNodeMessage(Message):
    __slots__ = {
        "target": "The node ID you are trying to find"
    }

    def __init__(self, compress: int = 0, seq: Optional[int] = None, sender: bytes = b'', target: bytes = b''):
        super().__init__(compress, MessageType.FIND_NODE, seq, sender)
        self.target = target

    @property
    def _data(self):
        return (*super()._data, self.target)

    def react(self, node, addr, sock):
        """Send back a list of GlobalPeerInfo objects representing up to the k closest nodes to the target."""
        node.logger.debug("Got a FIND_NODE from %r (%r)", addr, self)
        super().react(node, addr, sock)
        node._send(sock, addr, AckMessage(
            resp_seq=self.seq, data=tuple(peer.public for peer in node.routing_table.nearest(self.target).values())
        ))

    def react_response(self, ack, node, addr, sock, message_constructor=PingMessage):
        """Attempt to connect to each of the nodes you were told about."""
        node.logger.debug("Got a response to a FIND_NODE from %r (%r, %r)", addr, self, ack)
        for info in ack.data:
            name = info.name
            if name != node.id and name not in node.routing_table:
                for address in info.addresses:
                    try:
                        if node.routing_table.add(name, address.args, address.addr_type):
                            node._send(address.addr_type, address.args, message_constructor())
                            node.routing_table.member_info[name].public = info
                        break
                    except Exception:
                        continue


@Message.register(int(MessageType.FIND_KEY))
class FindKeyMessage(FindNodeMessage):
    __slots__ = {
        "key": "The actual key you're looking for, not just the hash",
        "async_res": "The async result you might be waiting for"
    }

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[int] = None,
        sender: bytes = b'',
        target: bytes = b'',
        key: bytes = b''
    ):
        super().__init__(compress, seq, sender, target)
        self.key = key
        self.message_type = MessageType.FIND_KEY
        self.async_res: 'Optional[Future[Any]]' = None

    @property
    def _data(self):
        return (*super()._data, self.key)

    def react(self, node, addr, sock):
        """If you are responsible for a key, send it's value.

        Otherwise send the requesting node a list of GlobalPeerInfo objects representing up to the k closest nodes to
        the requested address.
        """
        node.logger.debug("Got a FIND_KEY from %r (%r)", addr, self)
        if self.key in node.storage or self.target == node.id or \
           max(node.routing_table.nearest(self.target)) < distance(self.target, node.id):
            Message.react(self, node, addr, sock)
            node._send(sock, addr, AckMessage(resp_seq=self.seq, data=node.storage.get(self.key)))
        else:
            super().react(node, addr, sock)

    def react_response(self, ack, node, addr, sock):
        """Update the Future object related to the request."""
        if self.async_res is None:
            node.logger.debug("Got a duplicate response to a FIND_KEY from %r (%r, %r)", addr, self, ack)
        else:
            node.logger.debug("Got a response to a FIND_KEY from %r (%r, %r)", addr, self, ack)
            if isinstance(ack.data, list):
                super().react_response(ack, node, addr, sock, self.increment_seq)
            else:
                self.async_res.set_result(ack.data)
            self.async_res = None


@Message.register(int(MessageType.STORE_KEY))
class StoreKeyMessage(Message):
    __slots__ = {
        "target": "The value ID you are trying to set",
        "key": "The key you are trying to set",
        "value": "The value you are seeking to store"
    }

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[int] = None,
        sender: bytes = b'',
        target: bytes = b'',
        key: bytes = b'',
        value: Any = None
    ):
        super().__init__(compress, MessageType.STORE_KEY, seq, sender)
        self.target = target
        self.key = key
        self.value = value

    @property
    def _data(self):
        if self.value is None:
            return (*super()._data, self.target, self.key)
        return (*super()._data, self.target, self.key, self.value)

    def _schedule_val_expire(self, node, dist):
        if self.key in node.timeouts:
            node.schedule.cancel(node.timeouts[self.key])
        node.storage[self.key] = self.value
        if dist < max(node.routing_table.nearest(self.target)):
            rough_dist = 65  # minutes
        else:
            rough_dist = 120 / dist.bit_length()

        def timeout():
            node.logger.debug("Purging %r from local storage", self.key)
            del node.storage[self.key]
            del node.timeouts[self.key]

        node.timeouts[self.key] = node.schedule.enter(rough_dist, 1, timeout)

    def react(self, node, addr, sock):
        """If you're responsible for a key, store it, otherwise NACK."""
        node.logger.debug("Got a STORE_KEY from %r (%r)", addr, self)
        super().react(node, addr, sock)
        status = 0
        dist = distance(node.id, self.target)
        peers = [distance(name, self.target) for name in node.routing_table.member_info]
        if len(peers) < node.config.k:
            peers.sort()
            peers = peers[:node.config.k]
        if self.target in node.storage or len(peers) < node.config.k or peers[-1] > dist:
            try:
                node.schedule.cancel(node.timeouts[self.target])
            except KeyError:
                pass
            node.storage[self.target] = self.value
            self._schedule_val_expire(node, dist)
        else:
            status = -1
        node._send(sock, addr, AckMessage(resp_seq=self.seq, status=status))


@Message.register(int(MessageType.HELLO))
class HelloMessage(PingMessage):
    def __init__(
        self,
        compress: int = 0,
        seq: Optional[int] = None,
        sender: bytes = b'',
        kwargs: GlobalPeerInfo = None
    ):
        super().__init__(compress, seq, sender)
        self.message_type = MessageType.HELLO
        self.kwargs = kwargs

    @property
    def _data(self):
        return (*super()._data, self.kwargs)

    def react(self, node, addr, sock):
        """If you care about a node, record its global info and set local parameters accordingly."""
        node.logger.debug("Got a HELLO from %r (%r)", addr, self)
        if self.sender not in node.routing_table:
            node.routing_table.add(self.sender, addr, node.socks.index(sock))
        try:
            if self.sender in node.routing_table:
                member_info = node.routing_table.member_info[self.sender]
                member_info.public = self.kwargs
                member_info.local.compression = decide_compression(
                    self.kwargs.compression,
                    node.preferred_compression,
                    self.sender > node.id
                )
        except Exception:
            node._send(sock, addr, AckMessage(resp_seq=self.seq, status=-1))
            return
        node._send(sock, addr, AckMessage(resp_seq=self.seq))
        Message.react(self, node, addr, sock)


@Message.register(int(MessageType.IDENTIFY))
class IdentifyMessage(PingMessage):
    def __init__(self, compress: int = 0, seq: Optional[int] = None, sender: bytes = b''):
        super().__init__(compress, seq, sender)
        self.message_type = MessageType.IDENTIFY

    def react(self, node, addr, sock):
        """Send a HELLO back in leiu of an ACK."""
        node.logger.debug("Got an IDENTIFY request from %r (%r)", addr, self)
        node._send_hello(sock, addr)
        Message.react(self, node, addr, sock)


@Message.register(int(MessageType.BROADCAST))
class BroadcastMessage(Message):
    __slots__ = ('payload', )

    def __init__(self, compress: int = 0, seq: Optional[int] = None, sender: bytes = b'', payload: Any = None):
        super().__init__(0, MessageType.BROADCAST, seq, sender)
        self.payload = payload

    @property
    def _data(self):
        if self.payload is None:
            return super()._data
        return (*super()._data, self.payload)

    def react(self, node, addr, sock):
        """If unseen, propogate the message to your other peers.

        Broadcasting in this strategy takes about (n-1)^2 messages.
        """
        if (self.sender, self.seq) not in node.seen_broadcasts:
            node.logger.debug("Got a new BROADCAST from %r (%r)", addr, self)
            node.seen_broadcasts.add((self.sender, self.seq))
            for peer in node.routing_table:
                if peer.public.name == self.sender or peer.local.addr == addr:
                    continue
                node._send(node.socks[peer.local.sock], peer.local.addr, self)
        else:
            node.logger.debug("Got a repeat BROADCAST from %r (%r)", addr, self)
        super().react(node, addr, sock)
