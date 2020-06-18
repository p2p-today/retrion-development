"""An upgrade of the previous to provide support for a naive broadcast message type."""

from __future__ import annotations

import socket

from concurrent.futures import Future
from functools import partial
from hashlib import sha1
from itertools import chain
from logging import getLogger
from os import urandom
from queue import SimpleQueue
from sched import scheduler
from threading import Thread
from time import sleep
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

from umsgpack import ext_serializable, packb, unpackb

from .protocol.v04 import (distance, preferred_compression, Address, AddressType, BroadcastMessage, CompressType,
                           GlobalPeerInfo, HelloMessage, IdentifyMessage, Message, FindKeyMessage, FindNodeMessage,
                           PeerInfo, StoreKeyMessage)


@ext_serializable(3)
class NetworkConfiguration:
    """A dummy object that groups together network configuration parameters."""

    __slots__ = {
        "k": "REQUIRES GLOBAL AGREEMENT. The replication parameter. How many nodes are in each bucket, how many "
             "messages are sent per FIND/STORE request.",
        "b": "LOCAL ONLY. The accelleration parameter. How large of a prefix do we look for when dividing buckets? "
             "Routing table size is ~O(2^b * log_{2^b}(n)), lookup is ~O(log_{2^b}(n)).",
        "h": "REQUIRES GLOBAL AGREEMENT. The size of the hash function used in bytes.",
        "h_name": "REQUIRES GLOBAL AGREEMENT. The name of the hash function used.",
        "alpha": "The concurrency parameter. Defaults to k."
    }

    def __init__(self, k: int, b: int, h: int, h_name: str = "SHA1", alpha: Optional[int] = None):
        self.k = k
        self.b = b
        self.h = h
        self.h_name = h_name
        self.alpha = alpha or k


class RoutingTable:
    __slots__ = {
        'logger': 'A handle to the logging module',
        'config': 'A reference to your network configuration',
        'ref_id': 'The node you are routing for',
        'distance': 'A partial function which pre-fills your node id in the standard distance function',
        'table': 'The main routing table divided into h subgroups of 2^b - 1 buckets. Each bucket is a '
                 'mapping from id to sock index, addr pairs',
        'member_info': 'This mapping of id to PeerInfo objects keeps much richer data for each peer for easier access',
        'delay': 'The number of seconds you should next wait before looking for new nodes. Reset every time a node is '
                 'added or removed, doubles every time you look for them.'
    }

    def __init__(self, ref_id: bytes, config: NetworkConfiguration):
        self.logger = getLogger(f'KademliaNode[id={ref_id.hex()}].routing_table')
        self.config = config
        self.ref_id = ref_id
        self.distance = partial(distance, ref_id)
        self.table: Tuple[Tuple[Dict[bytes, Tuple[int, Tuple[Any, ...]]], ...], ...] = \
            tuple(tuple({} for _ in range((1 << config.b) - 1)) for _ in range(config.h * 8 // config.b + 1))
        self.member_info: Dict[bytes, PeerInfo] = {}
        self.delay = 60

    def add(self, name: bytes, addr: Tuple[Any, ...], sock: int) -> bool:
        """Add a node to your routing table, initially containing only the minimum viable information.

        To add more info, directly modify the PeerInfo object in routing_table.member_info.
        """
        if name == self.ref_id:
            self.logger.info("Tried to add myself to my routing table")
            return False
        dist = self.distance(name)
        if self.config.b == 1:
            row = self.table[dist.bit_length() - 1][0]
        else:
            # TODO: verify the math here. I might have an error somewhere in here.
            group = self.table[(dist.bit_length() - 1) // self.config.b]
            row = group[(dist - 1) % (1 << self.config.b)]
        if len(row) < self.config.k:
            self.logger.info("Added %s to our routing table", name.hex())
            row[name] = (sock, addr)
            self.member_info[name] = PeerInfo(name, addr, sock, self.ref_id)
            self.logger.debug("A node was added, so I'm resetting the delay to next look for peers")
            self.delay = 60
            return True
        self.logger.info("Would add %s to our routing table, but that bucket is full", name.hex())
        return False

    def remove(self, name: bytes) -> None:
        """Remove a node from your routing table, if it is present."""
        if name in self:
            self.logger.info("Removing %s from our routing table", name.hex())
            dist = self.distance(name)
            if self.config.b == 1:
                row = self.table[dist.bit_length() - 1][0]
            else:
                # TODO: verify the math here. I might have an error somewhere in here.
                group = self.table[(dist.bit_length() - 1) // self.config.b]
                row = group[(dist - 1) % (1 << self.config.b)]
            del row[name]
            del self.member_info[name]
            self.logger.debug("A node was removed, so I'm resetting the delay to next look for peers")
            self.delay = 60
        else:
            self.logger.info("Would remove %s from our routing table, but they aren't there", name.hex())

    def __contains__(self, addr: Union[bytes, Address, Tuple[Any, ...]]) -> bool:
        """Look up if a name, address, or tuple-formatted address is in your routing table."""
        if isinstance(addr, bytes):
            return addr in self.member_info
        if isinstance(addr, tuple):
            return (addr in (peer.local.addr for peer in self.member_info.values()))
        return addr in chain.from_iterable(peer.public.addresses for peer in self.member_info.values())

    def __iter__(self) -> Iterator[PeerInfo]:
        """Iterate through the routing table."""
        yield from self.member_info.values()

    def register_refresh(self) -> None:
        """Increase the delay between next looking for nodes."""
        self.logger.debug("A refresh event was triggered, so I'm doubling the delay to next look for peers")
        self.delay *= 2

    def by_address(self, addr: Union[Address, Tuple[Any, ...]]) -> Optional[PeerInfo]:
        """Look up a node in your routing table by their address."""
        if isinstance(addr, tuple):
            for peer in self.member_info.values():
                if addr == peer.local.addr:
                    return peer
                for address in peer.public.addresses:
                    if address.args == addr:
                        return peer
        for peer in self.member_info.values():
            if addr in peer.public.addresses:
                return peer
        return None

    def nearest(self, target: bytes) -> Dict[int, PeerInfo]:
        """Return up to the nearest k nodes to some target."""
        dist = self.distance(target)
        try:
            if self.config.b == 1:
                row = self.table[dist.bit_length() - 1][0]
            else:
                # TODO: verify the math here. I might have an error somewhere in here.
                group = self.table[(dist.bit_length() - 1) // self.config.b]
                row = group[(dist - 1) % (1 << self.config.b)]
            if len(row) == self.config.k:
                ret: Dict[int, PeerInfo] = {}
                for name in row:
                    ret[distance(target, name)] = self.member_info[name]
                return ret
        except IndexError:
            self.logger.exception("Hey, I hit that known bug again!")
        ret = {distance(target, x.public.name): x for x in self}
        if len(ret) > self.config.k:
            new_ret: Dict[int, PeerInfo] = {}
            while len(new_ret) < self.config.k:
                k = min(ret)
                new_ret[k] = ret.pop(k)
            ret = new_ret
        return ret


class KademliaNode:
    __slots__ = {
        'logger': 'A handle to the logging module',
        'config': 'A reference to your network configuration',
        'routing_table': 'A handle to your routing table',
        'id': 'The name of this node',
        'preferred_compression': 'The list of compression methods this node is able to use, in order of preference',
        'socks': 'The list of listening sockets for this node',
        'storage': 'The table which holds the keys you are responsible for holding, with entries clearing inversely '
                   'proportional to the square of its distance from you',
        'awaiting_ack': 'The list of messages waiting for an ACK',
        'timeouts': 'The list of timeout events to clear messages that would be waiting for ACKs',
        'daemons': 'The list of currently running daemons for this node (should be num_sockets + 2)',
        'addresses': 'The list of MessagePack-encodable addresses you are listening on',
        'seen_broadcasts': 'A set of sender, idx pairs of recently seen broadcast messages, used to filter the naive '
                           'broadcast algorithm',
        'message_queue': 'The queue of newly received messages for processing by the reactor thread',
        'schedule': 'The queue of upcoming events for the heartbeat thread to execute, largely pings and looking for '
                    'new nodes'
    }

    def __init__(self, port: int):
        self.config = NetworkConfiguration(k=2, b=3, h=20)
        self.id = urandom(self.config.h)
        self.logger = getLogger(f'KademliaNode[id={self.id.hex()}]')
        self.logger.debug("My preferred compression methods are %r", preferred_compression)
        self.preferred_compression = preferred_compression
        self.socks = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM),
                      socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)]
        self.socks[1].setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
        self.routing_table = RoutingTable(self.id, self.config)
        self.awaiting_ack: Dict[int, Message] = {}
        self.message_queue: SimpleQueue[Tuple[Message, Tuple[Any, ...], socket.socket]] = SimpleQueue()
        self.storage: Dict[bytes, Any] = {}
        self.timeouts: Dict[Union[bytes, Tuple[bytes]], Any] = {}
        self.daemons: List[Thread] = []
        self.addresses: List[Address] = []
        self.seen_broadcasts: Set[Tuple[bytes, int]] = set()
        for sock, addr, family, rand_addr in zip(
            self.socks,
            ('0.0.0.0', '::'),
            (socket.AF_INET, socket.AF_INET6),
            ('8.8.8.8', '2606:4700:4700::1111')
        ):
            self.logger.debug("Setting up a listening address on %r", addr)
            daemon = Thread(target=self.listen_loop, args=(sock, ), daemon=True)
            sock.bind((addr, port))
            daemon.start()
            self.daemons.append(daemon)
            s = socket.socket(family, socket.SOCK_DGRAM)
            s.connect((rand_addr, 1))
            local_ip_address = s.getsockname()[0]
            s.close()
            self.logger.debug("Discovered the actual listening address is %r", local_ip_address)
            self.addresses.append(Address(
                AddressType.UDPv4 if family == socket.AF_INET else AddressType.UDPv6,
                local_ip_address,
                port
            ))
        action_daemon = Thread(target=self.reactor, daemon=True)
        action_daemon.start()
        self.daemons.append(action_daemon)
        self.schedule = scheduler()
        self.schedule.enter(self.routing_table.delay, 0, self.refresh)
        self.timeouts = {}
        heartbeat_daemon = Thread(target=self.heartbeat, daemon=True)
        heartbeat_daemon.start()
        self.daemons.append(heartbeat_daemon)

    def refresh(self):
        """Search for new peers and clear ones which are largely unresponsive."""
        # TODO: Add pruning of nodes with high miss counts
        self.logger.debug("Refreshing lower-than-k buckets")
        self.routing_table.register_refresh()
        for bit_length, bucket_group in enumerate(self.routing_table.table):
            dist = bit_length * (self.config.b - 1)
            for prefix, bucket in enumerate(bucket_group, start=1):
                xored = (int.from_bytes(self.id, 'big') ^ ((prefix << dist))).to_bytes(self.config.h, 'big')
                if len(bucket) < self.config.k and self.config.k.bit_length() <= dist * self.config.b:
                    self.send_all(FindNodeMessage(target=xored))
        self.schedule.enter(self.routing_table.delay, 0, self.refresh)

    def listen_loop(self, sock: socket.socket):
        """Listen for incoming messages on a separate thread."""
        self.logger.info("Listen loop started for %r", sock)
        while True:
            try:
                data, addr = sock.recvfrom(4096)
                message = unpackb(data)
                self.message_queue.put((message, addr, sock))
            except Exception:
                self.logger.exception("Ran into an error in the listener loop for %r", sock)

    def reactor(self):
        """React to incoming messages on a separate thread."""
        self.logger.info("Reactor loop started")
        while True:
            try:
                message, addr, sock = self.message_queue.get()
                message.react(self, addr, sock)
                if message.sender not in self.routing_table:
                    if isinstance(message, BroadcastMessage):
                        if message.sender != self.id:
                            self._send(sock, addr, FindNodeMessage(target=message.sender))
                    else:
                        if self.routing_table.add(message.sender, addr, self.socks.index(sock)):
                            self._send_hello(sock, addr)
                            for key in self.storage:
                                target = sha1(key).digest()
                                nearest = self.routing_table.nearest(target)
                                if message.sender in (peer.public.name for peer in nearest.values()):
                                    self._send(
                                        sock, addr, StoreKeyMessage(target=target, key=key, value=self.storage[key])
                                    )
            except Exception:
                self.logger.exception("Ran into an error in the rector loop")

    def heartbeat(self):
        """Run through scheduled events in a separate thread."""
        self.logger.info("Heartbeat loop started")
        while True:
            try:
                cutoff = self.schedule.timefunc()
                self.schedule.run()
                for event in self.schedule.queue:
                    if event.time < cutoff:
                        self.schedule.cancel(event)
            except Exception:
                self.logger.exception("Encountered an error in the heartbeat loop")
            sleep(0.01)

    def connect(self, sock: int, addr: Tuple[Any, ...]):
        """Send an IDENTIFY message to your peer to kick off the connection process."""
        self._send(self.socks[sock], addr, IdentifyMessage())

    def _send(self, sock: socket.socket, addr: Tuple[Any, ...], message: Message):
        message.sender = self.id
        peer = self.routing_table.by_address(addr)
        if peer:
            message.compress = peer.local.compression
        else:
            message.compress = CompressType.PLAIN
        if not isinstance(message, IdentifyMessage):  # IDENTIFY doesn't want an ACK, it wants a HELLO
            self.awaiting_ack[message.nonce] = message

            def stale():
                """If a message wasn't ACK'd, mark a miss."""
                if message.nonce in self.awaiting_ack:
                    del self.awaiting_ack[message.nonce]
                    peer = self.routing_table.by_address(addr)
                    if peer:
                        peer.local.misses += 1

            self.schedule.enter(60, 100, stale)
        sock.sendto(packb(message), addr)

    def _send_hello(self, sock: socket.socket, addr: Tuple[Any, ...]):
        self._send(sock, addr, HelloMessage(kwargs=GlobalPeerInfo(self.id, self.addresses, self.preferred_compression)))

    def send_all(self, message: Message) -> int:
        """Send a message to every node in your routing table, returning the sum of how many were successful."""
        successful = 0
        for name in tuple(self.routing_table.member_info):
            successful += self.send(name, message.increment_seq())
        return successful

    def send(self, name: bytes, message: Message) -> bool:
        """Send a message to a particular node, returning True if successful, False if not.

        Raises
        ------
        RuntimeError: If you don't know how to reach a particular node, this function will issue FIND_NODE messages
        """
        if name not in self.routing_table:
            if name == self.id:
                return False
            self.routing_table.register_refresh()
            self.send_all(FindNodeMessage(target=name))
            raise RuntimeError("You don't have them in your contacts yet. Try again later.")
        sock = self.routing_table.member_info[name].local.sock
        addr = self.routing_table.member_info[name].local.addr
        self._send(self.socks[sock], addr, message)
        return True

    def get(self, key: bytes) -> 'Future[Any]':
        """Fetch a value from the distributed hash table."""
        target = sha1(key).digest()
        ret = Future()  # type: ignore
        if key in self.storage:
            ret.set_result(self.storage[key])
            return ret
        nearest = self.routing_table.nearest(target)
        if not nearest or max(nearest) > distance(self.id, target):
            ret.set_result(None)
            return ret
        # TODO: the requesting node stores the key at the closest node it saw to the key that didn't return the value
        msg = FindKeyMessage(target=target, key=key)
        msg.async_res = ret
        for peer in nearest.values():
            self._send(self.socks[peer.local.sock], peer.local.addr, msg.increment_seq())
        return ret

    def set(self, key: bytes, value: Any):
        """Set a value in the distributed hash table."""
        target = sha1(key).digest()
        dist = distance(self.id, target)
        msg = StoreKeyMessage(target=target, key=key, value=value)
        msg._schedule_val_expire(self, dist)  # this mutates our local storage
        for peer in self.routing_table.nearest(target).values():
            self._send(self.socks[peer.local.sock], peer.local.addr, msg.increment_seq())
