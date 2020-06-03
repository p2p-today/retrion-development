"""An upgrade of the naive implementation of Kademlia to provide significant improvements to the routing table."""

import hashlib

from concurrent.futures import Future
from copy import copy, deepcopy
from enum import IntEnum
from itertools import count
from socket import SocketType
from time import monotonic
from traceback import format_exc
from typing import cast, overload, Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, Union

from umsgpack import _ext_classes, ext_serializable, packb, unpackb

from . import preferred_compression, BaseMessage, CompressType  # noqa: F401
from ..kademlia_v05 import KademliaNode

PROTOCOL_VERSION: int = 5

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


@ext_serializable(1)
class Address:
    """Base class that represents addresses in GlobalPeerInfo objects."""

    __slots__ = ('addr_type', 'args')

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}{(self.addr_type, *self.args)}"

    def __deepcopy__(self, memo=None) -> 'Address':
        return unpackb(packb(self))

    def __init_subclass__(cls, **kwargs):
        """Ensure that subclasses are recognized by umsgpack."""
        _ext_classes[cls] = 1

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

    __slots__ = ('addr', 'port')

    def __init__(self, addr_type: AddressType, addr: str, port: int):
        super().__init__(addr_type, addr, port)
        self.addr = addr
        self.port = port

    def packb(self) -> bytes:
        """Pack a UDPv4Address as a MessagePack bytestring."""
        return packb((self.addr_type, self.addr, self.port))


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


@ext_serializable(2)
class NetworkConfiguration:
    """A dummy object that groups together network configuration parameters."""

    __slots__ = {
        "channel": "REQUIRES GLOBAL AGREEMENT (for now). The channel number this subnet communicates over.",
        "k": "REQUIRES GLOBAL AGREEMENT. The replication parameter. How many nodes are in each bucket, how many "
             "messages are sent per FIND/STORE request.",
        "b": "LOCAL ONLY. The accelleration parameter. How large of a prefix do we look for when dividing buckets? "
             "Routing table size is ~O(2^b * log_{2^b}(n)), lookup is ~O(log_{2^b}(n)).",
        "h": "REQUIRES GLOBAL AGREEMENT. The size of the hash function used in bytes.",
        "h_name": "REQUIRES GLOBAL AGREEMENT. The name of the hash function used.",
        "h_func": "REQUIRES GLOBAL AGREEMENT. The hash function used.",
        "dht_disabled": "REQUIRES GLOBAL AGREEMENT. This being true indicates that FIND_KEY and STORE are disabled.",
        "alpha": "LOCAL ONLY. The concurrency parameter. Defaults to k.",
        "supported": "Whether we can connect to such a network."
    }

    def __init__(
        self,
        channel: int,
        k: int,
        b: int,
        h_name: str = "SHA1",
        dht_disabled: bool = False,
        alpha: Optional[int] = None
    ):
        self.channel = channel
        self.k = k
        self.b = b
        self.alpha = alpha or k
        self.dht_disabled = dht_disabled
        self.h_name = h_name.lower()
        if self.h_name not in hashlib.algorithms_available:
            self.supported = False
            self.h = -1

            def foo(data: bytes):
                raise RuntimeError()

            self.h_func = foo
        else:
            self.supported = True
            try:
                self.h_func = getattr(hashlib, self.h_name)
            except Exception:
                self.h_func = lambda data: hashlib.new(self.h_name, data)
            self.h = self.h_func(b'').digest_size

    def equivalent(self, other: 'NetworkConfiguration') -> bool:
        """Return True if it only varies from the other network in local parameters."""
        return (self.k == other.k and self.h_name == other.h_name and self.dht_disabled == other.dht_disabled
                and self.channel == other.channel)

    @staticmethod
    def unpackb(data: bytes) -> 'NetworkConfiguration':
        """Pack a NetworkConfiguration as a MessagePack bytestring."""
        return NetworkConfiguration(*unpackb(data))

    def packb(self) -> bytes:
        """Given a MessagePack bytestring, reconstruct a NetworkConfiguration."""
        if self.alpha == self.k:
            if self.h_name == "SHA1":
                return packb((self.channel, self.k, self.b))
            return packb((self.channel, self.k, self.b, self.h_name))
        return packb((self.channel, self.k, self.b, self.h_name, self.alpha))


class PeerInfo:
    """Holds peer info, split into local and public."""

    __slots__ = {
        "local": "Information about the peer that only you need to know",
        "public": "Information about the peer that everyone knows"
    }

    def __init__(self, name: bytes, addr: Tuple[Any, ...], sock: int, your_id: bytes):
        self.local = LocalPeerInfo(sock, addr, CompressType.PLAIN, distance(name, your_id))
        self.public = GlobalPeerInfo([])


@ext_serializable(3)
class ChannelInfo:
    """Holds information relevant to a peer's channels."""

    __slots__ = {
        "name": "The name of the channel you're implementing",
        "description": "What does this channel do and/or a URL to a longer explanation",
        "id": "The ID of your peer on this channel",
        "subnet": "The network descriptor for this channel",
        "proprietary": "A mapping that holds node info related to applications and specific implementations",
    }

    def __init__(
        self,
        name: str,
        description: str,
        id_: bytes,
        subnet: NetworkConfiguration,
        proprietary: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.description = description
        self.id = id_
        self.subnet = subnet
        self.proprietary = proprietary

    def __repr__(self) -> str:
        """Return a string representation of the peer info."""
        return (f"ChannelInfo(name={self.name!r}, description={self.description!r}, id_={self.id!r}, "
                f"subnet={self.subnet}, proprietary={self.proprietary})")

    def packb(self) -> bytes:
        """Pack a ChannelInfo as a MessagePack bytestring."""
        return packb((self.name, self.description, self.id, self.subnet, self.proprietary))

    @classmethod
    def unpackb(cls, data: bytes) -> 'ChannelInfo':
        """Given a MessagePack bytestring, reconstruct a ChannelInfo."""
        return cls(*unpackb(data))


@ext_serializable(4)
class GlobalPeerInfo:
    """Holds the peer info that is available to all nodes."""

    __slots__ = {
        "addresses": "The addresses of your peer",
        "compression": "The supported compression modes of your peer",
        "channels": "The channels this node listens on mapped to their corresponding network description",
        "proprietary": "A mapping that holds node info related to applications and specific implementations",
    }

    def __init__(
        self,
        addresses: List[Address],
        compression: Optional[List[CompressType]] = None,
        channels: Dict[int, ChannelInfo] = None,
        proprietary: Optional[Dict[str, Any]] = None,
    ):
        self.addresses: List[Address] = addresses
        self.compression: List[CompressType] = [CompressType(x) for x in (compression or ())]
        if CompressType.PLAIN not in self.compression:
            self.compression.append(CompressType.PLAIN)
        self.channels = channels or {}
        self.proprietary = proprietary or {}

    def __repr__(self) -> str:
        """Return a string representation of the peer info."""
        return (f"GlobalPeerInfo(addresses={self.addresses}, compression={self.compression}, "
                f"channels={self.channels!r}, proprietary={self.proprietary}")

    def packb(self) -> bytes:
        """Pack a GlobalPeerInfo as a MessagePack bytestring."""
        return packb((self.addresses, self.compression, self.channels, self.proprietary))

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
        "channel": "The channel you send messages to this peer over",
    }

    def __init__(
        self,
        sock: int,
        addr: Tuple[Any, ...],
        compression: CompressType,
        distance: int,
        channel: Optional[int] = None
    ):
        self.sock = sock
        self.addr = addr
        self.compression = compression
        self.distance = distance
        self.first_seen = monotonic()
        self.misses = self.hits = 0
        self.channel = channel


class MessageType(IntEnum):
    ACK = 0
    PING = 1
    FIND_NODE = 2
    FIND_KEY = 3
    STORE_KEY = 4
    HELLO = 5
    IDENTIFY = 6
    BROADCAST = 7
    GOODBYE = 8


class Message(BaseMessage):
    __slots__ = {
        "seq": "The Hybrid Logical Clock timestamp of the message. Formerly the sequence number.",
        "channel": "The multiplex channel the message was sent on",
    }

    def with_time(self, t: Tuple[int, int]) -> 'Message':
        """Return a copy of the message with the specified HLC timestamp."""
        ret = copy(self)
        ret.seq = t
        return ret

    @staticmethod
    def reconstruct(compress: int, data: bytes):
        """Given a compression method and decompressed data, reconstruct a Message."""
        message_type, *rest = unpackb(data, use_tuple=True)
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
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0
    ):
        super().__init__(PROTOCOL_VERSION, compress, sender)
        self.message_type = message_type
        self.channel = channel
        self.seq = seq or (0, 0)

    @property
    def _data(self):
        return (self.message_type, self.seq, self.sender, self.channel)

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

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType):
        """Delay sending a PING to the sending node, since the connection is clearly active if you got a message."""
        try:
            event = node.timeouts[self.channel].pop((self.sender, ))
            node.schedule.cancel(event)
        except (KeyError, ValueError):
            pass

        def ping():
            node._send(sock, addr, PingMessage(channel=self.channel))

        node.timeouts[self.channel][(self.sender, )] = node.schedule.enter(60, 2, ping)

    def react_response(self, ack: 'AckMessage', node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType):
        """Try to make it prominent when messages that don't need an ack register them for one."""
        node.logger.warning("Hey! I called react_response on a message that didn't implement it! %r", self)


message_types: Dict[int, Type[Message]] = {}


@Message.register(int(MessageType.ACK))
class AckMessage(Message):
    __slots__ = {
        "resp_seq": "The Hybrid Logical Clock timestamp of the message you are responding to",
        "status": "The error number of the response (0 is good)",
        "data": "Any ancillary data to go with the message"
    }

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0,
        resp_seq: Tuple[int, int] = (0, 0),
        status: int = 0,
        data: Any = None
    ):
        super().__init__(compress, MessageType.ACK, seq, sender, channel)
        self.resp_seq: Tuple[int, int] = resp_seq
        self.status = status
        self.data = data

    @property
    def _data(self):
        if self.data is None:
            if self.status == 0:
                return (*super()._data, self.resp_seq)
            return (*super()._data, self.resp_seq, self.status)
        return (*super()._data, self.resp_seq, self.status, self.data)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType):
        """Clear the message from node.awaiting_ack and call the relevant react_response() method."""
        node.logger.debug("Got an %sACK from %r (%r)", 'N' if self.status else '', addr, self)
        super().react(node, addr, sock)
        try:
            node.routing_table[self.channel].member_info[self.sender].local.hits += 1
        except KeyError:
            pass
        if self.resp_seq in node.awaiting_ack:
            node.awaiting_ack[self.resp_seq].react_response(self, node, addr, sock)
            del node.awaiting_ack[self.resp_seq]


@Message.register(int(MessageType.PING))
class PingMessage(Message):
    __slots__ = ()

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0
    ):
        super().__init__(0, MessageType.PING, seq, sender, channel)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType):
        """Since it's just a ping, we just send an ACK."""
        node.logger.debug("Got a PING from %r (%r)", addr, self)
        super().react(node, addr, sock)
        node._send(sock, addr, AckMessage(resp_seq=self.seq, channel=self.channel))


@Message.register(int(MessageType.FIND_NODE))
class FindNodeMessage(Message):
    __slots__ = {
        "target": "The node ID you are trying to find"
    }

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0,
        target: bytes = b''
    ):
        super().__init__(compress, MessageType.FIND_NODE, seq, sender, channel)
        self.target = target

    @property
    def _data(self):
        return (*super()._data, self.target)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType, status=0):
        """Send back a list of GlobalPeerInfo objects representing up to the k closest nodes to the target."""
        node.logger.debug("Got a FIND_NODE from %r (%r)", addr, self)
        super().react(node, addr, sock)
        table = node.routing_table[self.channel]
        alpha = node.self_info.channels[self.channel].subnet.alpha
        node._send(sock, addr, AckMessage(
            resp_seq=self.seq,
            status=status,
            data=tuple(peer.public for peer in table.nearest(self.target, alpha).values()),
            channel=self.channel
        ))

    def react_response(
        self,
        ack: AckMessage,
        node: KademliaNode,
        addr: Tuple[Any, ...],
        sock: SocketType,
        message_constructor=PingMessage
    ):
        """Attempt to connect to each of the nodes you were told about."""
        node.logger.debug("Got a response to a FIND_NODE from %r (%r, %r)", addr, self, ack)
        for info in cast(Sequence[GlobalPeerInfo], ack.data):
            for channel, channel_info in info.channels.items():
                if not channel_info.subnet.supported or channel not in node.self_info.channels:
                    continue
                my_channel_info = node.self_info.channels[channel]
                if channel_info.subnet.equivalent(my_channel_info.subnet):
                    name = channel_info.id
                    if name != node.self_info.channels[channel].id and name not in node.routing_table[channel]:
                        for address in info.addresses:
                            try:
                                if node.routing_table[channel].add(name, address.args, address.addr_type):
                                    node._send(
                                        node.socks[address.addr_type],
                                        address.args,
                                        message_constructor()
                                    )
                                    node.routing_table[channel].member_info[name].public = info
                                break
                            except Exception:
                                node.errors.append(format_exc())
                                node.logger.exception("I was unable to send a message to %r", addr)


@Message.register(int(MessageType.FIND_KEY))
class FindKeyMessage(FindNodeMessage):
    __slots__ = {
        "key": "The actual key you're looking for, not just the hash",
        "async_res": "The async result you might be waiting for",
        "distances": "Distances of nodes I have received a response from thus far",
    }

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0,
        target: bytes = b'',
        key: bytes = b''
    ):
        super().__init__(compress, seq, sender, channel, target)
        self.key = key
        self.message_type = MessageType.FIND_KEY
        self.async_res: 'Optional[Future[Any]]' = None
        self.distances: Dict[int, Tuple[Tuple[Any, ...], int]] = {}

    @property
    def _data(self):
        return (*super()._data, self.key)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType, status=0):
        """If you are responsible for a key, send it's value.

        Otherwise send the requesting node a list of GlobalPeerInfo objects representing up to the k closest nodes to
        the requested address.
        """
        node.logger.debug("Got a FIND_KEY from %r (%r)", addr, self)
        Message.react(self, node, addr, sock)
        channel = self.channel
        channel_info = node.self_info.channels[channel]
        subnet = channel_info.subnet
        alpha = subnet.alpha
        if subnet.dht_disabled:
            node._send(
                sock,
                addr,
                AckMessage(resp_seq=self.seq, status=2, channel=channel)
            )
            raise PermissionError("You can't store data on this channel!")
        responsible = False
        if self.key in node.storage[channel] or self.target == channel_info.id:
            responsible = True
        if not responsible:
            nearest = node.routing_table[channel].nearest(self.target, alpha)
            if not nearest or max(nearest) < distance(self.target, channel_info.id):
                responsible = True
        if responsible:
            node._send(
                sock,
                addr,
                AckMessage(resp_seq=self.seq, data=node.storage[channel].get(self.key), channel=channel)
            )
        else:
            super().react(node, addr, sock, status=1)

    def react_response(
        self,
        ack: AckMessage,
        node: KademliaNode,
        addr: Tuple[Any, ...],
        sock: SocketType,
        message_constructor=PingMessage
    ):
        """Update the Future object related to the request."""
        if self.async_res is None:
            node.logger.debug("Got a duplicate response to a FIND_KEY from %r (%r, %r)", addr, self, ack)
        else:
            node.logger.debug("Got a response to a FIND_KEY from %r (%r, %r)", addr, self, ack)
            if ack.status == 1:
                dist = distance(self.target, ack.sender)
                self.distances[dist] = (addr, sock)
                super().react_response(ack, node, addr, sock, lambda: self.with_time(node.tick()))
            else:
                node.storage[self.channel][self.key] = ack.data
                msg = StoreKeyMessage(
                    target=self.target,
                    key=self.key,
                    value=ack.data,
                    channel=self.channel
                ).with_time(node.tick())
                msg._schedule_val_expire(  # type: ignore
                    node,
                    distance(self.target, node.self_info.channels[self.channel].id)
                )
                node._send(sock, addr, msg)
                self.async_res.set_result(deepcopy(ack.data))
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
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0,
        target: bytes = b'',
        key: bytes = b'',
        value: Any = None
    ):
        super().__init__(compress, MessageType.STORE_KEY, seq, sender, channel)
        self.target = target
        self.key = key
        self.value = value

    @property
    def _data(self):
        if self.value is None:
            return (*super()._data, self.target, self.key)
        return (*super()._data, self.target, self.key, self.value)

    def _schedule_val_expire(self, node, dist, originator=False):
        if self.key in node.timeouts:
            node.schedule.cancel(node.timeouts[self.key])
        node.storage[self.channel][self.key] = self.value
        nearest = node.routing_table[self.channel].nearest(self.target)
        if originator:
            rough_dist = 60  # minutes
        elif not nearest or dist < max(nearest):
            rough_dist = 65
        else:
            rough_dist = 120 / dist.bit_length()

        def timeout():
            if originator:
                node.logger.debug("Deciding if I should republish %r", self.key)
                new_value = node.get(self.key, self.channel, use_local_storage=False)

                @new_value.add_done_callback
                def should_republish(*args):
                    if not new_value.done() or new_value.result() == self.value:
                        node.logger.debug("Republishing %r", self.key)
                        node.set(self.key, self.value, self.channel)

                node.schedule.enter(60, 1, should_republish)
            else:
                node.logger.debug("Purging %r from local storage", self.key)
                del node.storage[self.channel][self.key]
                del node.timeouts[self.key]

        node.timeouts[self.key] = node.schedule.enter(rough_dist * 60, 1, timeout)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType):
        """If you're responsible for a key, store it, otherwise NACK."""
        node.logger.debug("Got a STORE_KEY from %r (%r)", addr, self)
        super().react(node, addr, sock)
        subnet = node.self_info.channels[self.channel].subnet
        status = 0
        if subnet.dht_disabled:
            status = 2
        else:
            peers = list(node.routing_table[self.channel].nearest(self.target))
            dist = distance(node.self_info.channels[self.channel].id, self.target)
            if len(peers) < subnet.k:
                peers.sort()
                peers = peers[:subnet.k]
            if self.target in node.storage[self.channel] or len(peers) < subnet.k or peers[-1] > dist:
                try:
                    node.schedule.cancel(node.timeouts[self.channel].pop(self.target))
                except KeyError:
                    pass
                node.storage[self.channel][self.key] = self.value
                self._schedule_val_expire(node, dist)
            else:
                status = -1
        node._send(sock, addr, AckMessage(resp_seq=self.seq, status=status, channel=self.channel))
        if status == 2:
            raise PermissionError("You can't store data on this channel!")


@Message.register(int(MessageType.HELLO))
class HelloMessage(PingMessage):
    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0,
        kwargs: GlobalPeerInfo = None
    ):
        super().__init__(compress, seq, sender, channel)
        self.message_type = MessageType.HELLO
        if kwargs is None:
            raise TypeError("Missing required argument: kwargs")
        self.kwargs = kwargs

    @property
    def _data(self):
        return (*super()._data, self.kwargs)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType):
        """If you care about a node, record its global info and set local parameters accordingly."""
        node.logger.debug("Got a HELLO from %r (%r)", addr, self)
        if self.sender not in node.routing_table[self.channel]:
            node.routing_table[self.channel].add(self.sender, addr, node.socks.index(sock))
        try:
            if self.sender in node.routing_table[self.channel]:
                member_info = node.routing_table[self.channel].member_info[self.sender]
                member_info.public = self.kwargs
                member_info.local.compression = CompressType(decide_compression(
                    self.kwargs.compression,
                    node.preferred_compression,
                    self.sender > node.self_info.channels[self.channel].id
                ))
                node.routing_table[self.channel].member_info[self.sender] = member_info
        except Exception:
            node._send(sock, addr, AckMessage(resp_seq=self.seq, status=-1, channel=self.channel))
            node.errors.append(format_exc())
            node.logger.exception("I ran into an issue in HELLO.react() %r", self)
            return
        node._send(sock, addr, AckMessage(resp_seq=self.seq, channel=self.channel))
        Message.react(self, node, addr, sock)


@Message.register(int(MessageType.IDENTIFY))
class IdentifyMessage(Message):
    __slots__ = ()

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0
    ):
        super().__init__(0, MessageType.IDENTIFY, seq, sender, channel)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType):
        """Send a HELLO back in leiu of an ACK."""
        node.logger.debug("Got an IDENTIFY request from %r (%r)", addr, self)
        super().react(node, addr, sock)
        node._send_hello(sock, addr, self.channel)


@Message.register(int(MessageType.BROADCAST))
class BroadcastMessage(Message):
    __slots__ = ('payload', )

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0,
        payload: Any = None
    ):
        super().__init__(compress, MessageType.BROADCAST, seq, sender, channel)
        self.payload = payload

    @property
    def _data(self):
        if self.payload is None:
            return super()._data
        return (*super()._data, self.payload)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType) -> bool:
        """If unseen, propogate the message to your other peers.

        Broadcasting in this strategy will have n nodes send O(k * 2^b * log_{2^b}(n)) messages, assuming all nodes
        agree on b. Note that early on this will appear to scale O(n^2), so for small groups consider using multicast
        messages instead. This transition usually occurs around 20-80 nodes, but please consult your particular
        values of k, b, and h to be sure. It should be approximately (n-1)^2 = 2^b * hk * log_{2^b}(n)

        Note
        ----
        Unlike other react() methods, this one has a meaningful return. True indicates that it is a new message, False
        that it's a repeat.
        """
        super().react(node, addr, sock)
        if (self.sender, self.seq) not in node.seen_broadcasts:
            node.logger.debug("Got a new BROADCAST from %r (%r)", addr, self)
            node.seen_broadcasts.add((self.sender, self.seq))
            for peer in node.routing_table[self.channel]:
                if peer.local.addr == addr:
                    continue
                if self.channel in peer.public.channels and peer.public.channels[self.channel].id == self.sender:
                    continue
                node._send(node.socks[peer.local.sock], peer.local.addr, self, peer=peer)
            return True
        node.logger.debug("Got a repeat BROADCAST from %r (%r)", addr, self)
        return False


@Message.register(int(MessageType.GOODBYE))
class GoodbyeMessage(Message):
    __slots__ = ()

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0
    ):
        super().__init__(0, MessageType.GOODBYE, seq, sender, channel)

    def react(self, node: KademliaNode, addr: Tuple[Any, ...], sock: SocketType):
        """Since it's just a GOODBYE, we get to delete everything about them."""
        node.logger.debug("Got a GOODBYE from %r (%r)", addr, self)
        try:
            event = node.timeouts[self.channel].pop((self.sender, ))
            node.schedule.cancel(event)
        except (KeyError, ValueError):
            pass

        node.routing_table[self.channel].remove(self.sender)
