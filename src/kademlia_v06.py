"""An upgrade of the previous to provide support for a naive broadcast message type."""

from __future__ import annotations

import socket

from collections import defaultdict
from copy import deepcopy
from concurrent.futures import Future
from datetime import datetime, timezone
from functools import partial
from itertools import chain
from logging import getLogger
from os import urandom
from random import choices
from sched import scheduler
from traceback import format_exc
from threading import Thread
from time import sleep, time
from typing import Any, DefaultDict, Dict, Iterator, List, Optional, Set, Tuple, Union

try:
    from queue import SimpleQueue  # added in 3.7
except ImportError:
    from queue import Queue as SimpleQueue  # type: ignore

from umsgpack import packb, unpackb

from .protocol.v06 import (distance, preferred_compression, AckMessage, Address, AddressType, BroadcastMessage,
                           ChannelInfo, CompressType, FloodMessage, GlobalPeerInfo, GoodbyeMessage, HelloMessage,
                           IdentifyMessage, Message, NetworkConfiguration, FindKeyMessage, FindNodeMessage, PeerInfo,
                           StoreKeyMessage)

# TODO: what happens if someone has a local address as the first item in addresses? do you send a message to them,
# not see an exception, and just think they're not listening? There needs to be some mechanism to handle this.
# Maybe I should send to all of them and have it so that nodes use the one they last heard on
# Alternately, I could have a timeout mechanism so that if in some timeout period it didn't hear back it tries the next
# address in the list. This list could be shuffled either randomly or by preference or not at all.

# TODO: support multiple routing tables so nodes can listen on two or more networks
# this would allow you to help maintain the bootstrap network while joining your own, for instance.
# might be reasonable to make "channels" in my incoming messages to determine which network it's for
# Maybe we do a thing where the things in FindNode are a mapping of NetworkConfiguration to GlobalNodeInfo,
# then nodes can use it to fill all of their routing tables in fewer messages.
# should clock sync include all listening networks or be specific to a channel? yes
# Make sure to note in protocol docs that nodes without multi-channel support should be possible
# outline the four example combinations and how they would work of (mono, multi) x (threaded, event loop)

# this is in progress:
# - [x] Add channel to message
# - [x] Add to GlobalPeerInfo somehow
# - [x] Remove single ID from GlobalPeerInfo
# - [x] Add storage[channel]
# - [x] Add channel to send APIs
# - [x] Add multiple ID support
# - [x] Add multiple routing table support
# - [x] Add bootstrap network support
# - [ ] Allow for nodes to have different channel numbers for the same network

# TODO: Add pruning of nodes with high miss counts

# TODO: Actually spawn instances on the servers I listed

# TODO: Add multicast messages support
# progress
# - [ ] Add target field to Messages
# - [ ] Add support for standard routed messages
#       If you have seen this message before, disregard it
#       If your ID is target, process without assuming the address is correct
#       If it isn't, find the alpha closest nodes to target and send to them
#       If the target is in said list, send only to target
# - [ ] Add support for multicast with the following algorithm
#       Sender calculates closest alpha nodes to each target and notes which are associated with which
#       Treat as if it was a routed message for each target, except that the responses should contain lists of peers
#       When forwarding, join messages together where possible. For instance, if 1000101..., is closest to targets A
#          b, then send targets as [a, b]
#       note that both this and the above are terrible algorithms that shouldn't stay around for too long
#       produces |targets| * alpha ^ log_{2^b}(n) messages in the worst case, which is potentially very high

# Overall Progress:

# - [x] An object model and serialization standard that can be used easily in most languages
# - [x] A simple high-level API (along with a fine-grained one)
# - [x] log(n) distributed hash table get/sets
# - [x] Support for disabling DHT support network-wide
# - [ ] Support for "edge nodes" which do not store DHT data except as a cache
# - [x] log(n) broadcast delivery time
# - [ ] log(n) routed message delivery
# - [ ] Support for multicast routed messages
# - [ ] Support for in-protocol group messages
# - [x] Transparent (to the developer) compression
# - [ ] transparent (to the user) encryption
# - [ ] Support for multiple transport protocols (TCP, UDP, etc.)
# - [ ] Support for "virtual transports" (using other nodes as a bridge)
# - [x] A well-defined standard configuration space
# - [x] Support for extension subprotocols and implementation-specific configurations

bootstrap_info = NetworkConfiguration(channel=-1, k=6, b=4, h_name="SHA256")
bootstrap_seeds = (
    (0, ('euclid.nmu.edu', 44565)),
    (0, ('williac.nmu.edu', 44565)),
    (0, ('acai.host.gabeappleton.me', 44565)),
    (0, ('bato.host.gabeappleton.me', 44565)),
)


def time_func() -> int:
    """Return milliseconds since UTC epoch."""
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


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
            tuple(tuple({} for _ in range(1 << config.b)) for _ in range(config.h * 8 // config.b + 1))
        self.member_info: Dict[bytes, PeerInfo] = {}
        self.delay = 60

    def add(self, name: bytes, addr: Tuple[Any, ...], sock: int) -> bool:
        """Add a node to your routing table, initially containing only the minimum viable information.

        To add more info, directly modify the PeerInfo object in routing_table.member_info.
        """
        if name == self.ref_id:
            self.logger.info("Tried to add myself to my routing table")
            return False
        elif name in self.member_info:
            self.logger.info("Tried to add a node that's already in the table for some reason")
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

    def nearest(self, target: bytes, amount: Optional[int] = None) -> Dict[int, PeerInfo]:
        """Return up to the nearest k nodes to some target."""
        if amount is None:
            amount = self.config.k
        dist = self.distance(target)
        try:
            if self.config.b == 1:
                row = self.table[dist.bit_length() - 1][0]
            else:
                # TODO: verify the math here. I might have an error somewhere in here.
                group = self.table[(dist.bit_length() - 1) // self.config.b]
                row = group[(dist - 1) % (1 << self.config.b)]
            if len(row) >= amount:
                ret: Dict[int, PeerInfo] = {}
                for name in sorted(row, key=lambda x: distance(x, target))[:amount]:
                    ret[distance(target, name)] = self.member_info[name]
                return ret
        except IndexError:
            self.logger.exception("Hey, I hit that known bug again!")
        ret = {
            distance(target, x.public.channels[self.config.channel].id): x for x in self
            if self.config.channel in x.public.channels
        }
        if len(ret) > self.config.k:
            new_ret: Dict[int, PeerInfo] = {}
            while len(new_ret) < amount:
                k = min(ret)
                new_ret[k] = ret.pop(k)
            ret = new_ret
        return ret


class KademliaNode:
    __slots__ = {
        'logger': 'A handle to the logging module',
        'routing_table': 'A handle to your routing table',
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
                    'new nodes',
        '_local_time': 'The local HLC timestamp of this node',
        'errors': 'The formatted error and tracebacks of problems this node has encountered in its daemons',
        'self_info': 'The GlobalPeerInfo object describing this node',
    }

    def __init__(self, port: int):
        self.errors: List[str] = []
        self._local_time: Tuple[int, int] = (time_func(), 0)
        self.logger = getLogger(f'KademliaNode[id={id(self)}]')
        self.logger.debug("My preferred compression methods are %r", preferred_compression)
        self.preferred_compression = preferred_compression
        self.schedule = scheduler()
        self.socks = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM),
                      socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)]
        self.socks[1].setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
        self.routing_table: Dict[int, RoutingTable] = {}
        self.awaiting_ack: Dict[Tuple[int, int], Message] = {}
        self.message_queue: SimpleQueue[Tuple[Message, Tuple[Any, ...], socket.socket]] = SimpleQueue()
        self.storage: DefaultDict[int, Dict[bytes, Any]] = defaultdict(dict)
        self.timeouts: DefaultDict[int, Dict[Union[bytes, Tuple[bytes]], Any]] = defaultdict(dict)
        self.daemons: List[Thread] = []
        self.addresses: List[Address] = []
        self.seen_broadcasts: Set[Tuple[bytes, Tuple[int, int]]] = set()
        self.self_info = GlobalPeerInfo(
            addresses=self.addresses,
            compression=preferred_compression
        )
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
        heartbeat_daemon = Thread(target=self.heartbeat, daemon=True)
        heartbeat_daemon.start()
        self.daemons.append(heartbeat_daemon)

    def tick(self) -> Tuple[int, int]:
        """Advance the local network clock, returning the new timestamp.

        Note, this returns a Hybrid Logical Clock timestamp, not an integer. To use this like physical time, just grab
        the front value and divide by 1000 to get seconds.
        """
        new_t = time_func()
        if self._local_time[0] < new_t:
            self._local_time = (new_t, 0)
        else:
            self._local_time = (self._local_time[0], self._local_time[1] + 1)
        return self._local_time

    def refresh(self, channel: int):
        """Search for new peers and clear ones which are largely unresponsive."""
        self.logger.debug("Refreshing lower-than-k buckets")
        self.routing_table[channel].register_refresh()
        for bit_length, bucket_group in enumerate(self.routing_table[channel].table):
            channel_info = self.self_info.channels[channel]
            dist = bit_length * (self.self_info.channels[channel].subnet.b - 1)
            for prefix, bucket in enumerate(bucket_group, start=1):
                subnet = channel_info.subnet
                xored = (int.from_bytes(channel_info.id, 'big') ^ ((prefix << dist))).to_bytes(subnet.h, 'big')
                if len(bucket) < subnet.k and subnet.k.bit_length() <= dist * subnet.b:
                    self.send_all(FindNodeMessage(target=xored))
        self.schedule.enter(self.routing_table[channel].delay, 0, self.refresh, argument=(channel, ))

    def listen_loop(self, sock: socket.socket):
        """Listen for incoming messages on a separate thread."""
        self.logger.info("Listen loop started for %r", sock)
        while True:
            try:
                data, addr = sock.recvfrom(4096)
                message = unpackb(data)
                self.message_queue.put((message, addr, sock))
            except Exception:
                self.errors.append(format_exc())
                self.logger.exception("Ran into an error in the listener loop for %r", sock)

    def reactor(self):
        """React to incoming messages on a separate thread."""
        self.logger.info("Reactor loop started")
        while True:
            try:
                message, addr, sock = self.message_queue.get()
                channel = message.channel
                # update local time
                physical = max((self._local_time[0], message.seq[0], time_func()))
                if physical in (self._local_time[0], message.seq[0]):
                    logical = max((self._local_time, message.seq))[0] + 1
                else:
                    logical = 0
                self._local_time = (physical, logical)
                # react to the message
                if channel not in self.self_info.channels:
                    self.logger.info("I got a message on closed channel %i", channel)
                    self._send(sock, addr, AckMessage(status=0xff, channel=channel))
                    continue
                message.react(self, addr, sock)
                # check if we need to introduce ourselves
                if message.sender not in self.routing_table[channel]:
                    if isinstance(message, FloodMessage):
                        if message.sender != self.self_info.channels[channel].id:
                            self._send(sock, addr, FindNodeMessage(target=message.sender, channel=channel))
                    else:
                        if self.routing_table[channel].add(message.sender, addr, self.socks.index(sock)):
                            self._send_hello(sock, addr, channel)
                            for key in self.storage[channel]:
                                target = self.self_info.channels[channel].subnet.h_func(key).digest()
                                nearest = self.routing_table[channel].nearest(target)
                                if message.sender in (peer.public.name for peer in nearest.values()):
                                    self._send(
                                        sock, addr, StoreKeyMessage(
                                            target=target,
                                            key=key,
                                            value=self.storage[channel][key],
                                            channel=channel
                                        )
                                    )
            except Exception:
                self.errors.append(format_exc())
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
                self.errors.append(format_exc())
                self.logger.exception("Encountered an error in the heartbeat loop")
            sleep(0.01)

    def is_active(self, channel: int, blocking: bool = False, timeout: float = float('nan')) -> bool:
        """Return a bool indicating if a channel is active or not.

        Optionally, block until it is active or the timeout expires. An active channel is defined as one that is both
        registered and has active connections to other nodes.
        """
        if channel not in self.routing_table:
            return False
        if blocking:
            timeout += time()
            while not self.routing_table[channel].member_info and timeout > time():
                sleep(0.01)
        return bool(self.routing_table[channel].member_info)

    def register_channel(
        self,
        name: str,
        description: str,
        subnet: NetworkConfiguration,
        proprietary: Optional[Dict[str, Any]] = None
    ) -> None:
        """Register a multiplex channel on this node."""
        if subnet.channel in self.self_info.channels:
            self.logger.error("Someone tried to register channel %r, but it's taken!", subnet.channel)
            raise ValueError("That channel is already assigned, try again.")
        id_ = urandom(subnet.h)  # TODO: replace this with something deterministic
        self.self_info.channels[subnet.channel] = ChannelInfo(name, description, id_, subnet, proprietary)
        self.routing_table[subnet.channel] = RoutingTable(id_, subnet)
        self.schedule.enter(self.routing_table[subnet.channel].delay, 0, self.refresh, argument=(subnet.channel, ))

    def bootstrap(self, channel_id: int, blocking: bool = False, timeout: float = float('inf')) -> bool:
        """Attempt to bootstrap a connection to your desired channel.

        Note that you must register the channel first. It is highly recommended that you keep a connection to the
        bootstrap network afterwards, as otherwise it will likely have very few nodes or your addition to the list of
        potential peers will expire.

        Note
        ----
        Regardless of blocking/timeout, this function may block for up to one second.
        """
        start = time()
        if bootstrap_info not in self.self_info.channels.values():
            self.register_channel(
                name="Boostrapper",
                description="A global network for Retrion peerfinding",
                subnet=bootstrap_info
            )
            for sock, addr in bootstrap_seeds:
                try:
                    self.connect(sock, addr, bootstrap_info.channel)
                except Exception:
                    self.logger.exception("Could not connect to %r", addr)
            if not self.is_active(bootstrap_info.channel, blocking=True, timeout=max(timeout, 1)):
                raise RuntimeError("Could not connect to the bootstrap network in a reasonable timeframe. Try again?")
            self.refresh(bootstrap_info.channel)

        channel = self.self_info.channels[channel_id]
        channel_info = (channel.name, channel.subnet.channel, channel.subnet.k, channel.subnet.h_name,
                        channel.subnet.dht_disabled)
        key = packb(channel_info)
        peers = self.get(key, channel=bootstrap_info.channel)

        def bootstrapper(promise):
            if promise.cancelled() or promise.result() is None:
                self.logger.info("There don't seem to be any nodes on the network")
                result: List[GlobalPeerInfo] = []
            else:
                result = promise.result()
                # below is basically a copy/paste of FindNodeMessage.react()
                for info in result:
                    for channel, channel_info in info.channels.items():
                        if not channel_info.subnet.supported or channel not in self.self_info.channels:
                            continue
                        my_channel_info = self.self_info.channels[channel]
                        if channel_info.subnet.equivalent(my_channel_info.subnet):
                            name = channel_info.id
                            if name != self.self_info.channels[channel].id and name not in self.routing_table[channel]:
                                for address in info.addresses:
                                    try:
                                        if self.routing_table[channel].add(name, address.args, address.addr_type):
                                            self.connect(
                                                address.addr_type,
                                                address.args,
                                                channel_id
                                            )
                                            self.routing_table[channel].member_info[name].public = info
                                        break
                                    except Exception:
                                        self.errors.append(format_exc())
                                        self.logger.exception("I was unable to send a message to %r", address)
            result = (self.self_info, *result)
            self.set(key, result, channel=bootstrap_info.channel)

        peers.add_done_callback(bootstrapper)
        timeout -= time() - start
        return self.is_active(channel_id, blocking, timeout)

    def connect(self, sock: int, addr: Tuple[Any, ...], channel: int):
        """Send an IDENTIFY message to your peer to kick off the connection process."""
        self._send(self.socks[sock], addr, IdentifyMessage(channel=channel))

    def close(self, channel: Optional[int] = None, *more: int):
        """Turn off one, multiple, or all of your channels."""
        if channel is None:
            self.close(*self.self_info.channels)
            return
        if more != ():
            self.close(*more)
        del self.self_info.channels[channel]
        for key, value in self.storage[channel].items():
            self.set(key, value, channel)
        self.send_all(GoodbyeMessage(channel=channel))
        del self.routing_table[channel]

    def _send(
        self,
        sock: socket.socket,
        addr: Tuple[Any, ...],
        message: Message,
        channel: Optional[int] = None,
        peer: Optional[PeerInfo] = None,
        do_tick: bool = True,
        originator: bool = True
    ):
        if channel is not None:
            message.channel = channel
        else:
            channel = message.channel
        if originator:
            message.sender = self.self_info.channels[channel].id
        if do_tick:
            message = message.with_time(self.tick())
        if peer is None:
            peer = self.routing_table[channel].by_address(addr)
        if peer:
            message.compress = peer.local.compression
        else:
            message.compress = CompressType.PLAIN
        # ACKs don't want an ACK
        # IDENTIFY doesn't want an ACK, it wants a HELLO
        # It's impractical to ACK a BROADCAST, just assume they got it
        if not isinstance(message, (AckMessage, IdentifyMessage, FloodMessage, BroadcastMessage)):
            self.awaiting_ack[message.seq] = message

            def stale():
                """If a message wasn't ACK'd, mark a miss."""
                if message.seq in self.awaiting_ack:
                    del self.awaiting_ack[message.seq]
                    peer = self.routing_table[channel].by_address(addr)
                    if peer:
                        peer.local.misses += 1

            self.schedule.enter(60, 100, stale)
        sock.sendto(packb(message), addr)

    def _send_hello(self, sock: socket.socket, addr: Tuple[Any, ...], channel: int):
        self._send(sock, addr, HelloMessage(kwargs=self.self_info, channel=channel))

    def send_all(
        self,
        message: Message,
        channel: Optional[int] = None,
        do_tick: bool = False,
        originator: bool = True
    ) -> int:
        """Send a message to every node in your routing table, returning the sum of how many were successful."""
        if channel is not None:
            message.channel = channel
        else:
            channel = message.channel
        successful = 0
        for name in tuple(self.routing_table[channel].member_info):
            successful += self.send(name, message, do_tick=do_tick, originator=originator)
        return successful

    def send(
        self,
        name: bytes,
        message: Message,
        channel: Optional[int] = None,
        do_tick: bool = True,
        originator: bool = True
    ) -> bool:
        """Send a message to a particular node, returning True if successful, False if not.

        Raises
        ------
        RuntimeError: If you don't know how to reach a particular node, this function will issue FIND_NODE messages
        """
        if channel is not None:
            message.channel = channel
        else:
            channel = message.channel
        if name not in self.routing_table[channel]:
            if name == self.self_info.channels[channel].id:
                return False
            alpha = self.self_info.channels[channel].subnet.alpha
            for peer in self.routing_table[channel].nearest(name, alpha).values():
                self.send(peer.public.channels[channel].id, FindNodeMessage(target=name, channel=channel))
            raise RuntimeError("You don't have them in your contacts yet. Try again later.")
        peer_info = self.routing_table[channel].member_info[name].local
        self._send(self.socks[peer_info.sock], peer_info.addr, message, do_tick=do_tick, originator=originator)
        return True

    def send_to(
        self,
        message: Message,
        target: bytes,
        *extra_targets: bytes,
        channel: Optional[int] = None,
        do_tick: bool = True,
        originator: bool = True
    ):
        if channel is not None:
            message.channel = channel
        else:
            channel = message.channel
        if not extra_targets:
            return self.send(target, message, do_tick=do_tick, originator=originator)
        raise NotImplementedError()

    def get(self, key: bytes, channel: int = 0, use_local_storage: bool = True) -> 'Future[Any]':
        """Fetch a value from the distributed hash table.

        Raises
        ------
        - PermissionError: If DHT features are disabled on the requested channel
        """
        subnet = self.self_info.channels[channel].subnet
        if subnet.dht_disabled:
            raise PermissionError("DHT features are disabled on this channel.")
        target = subnet.h_func(key).digest()
        ret = Future()  # type: ignore
        if use_local_storage and key in self.storage[channel]:
            ret.set_result(deepcopy(self.storage[channel][key]))
            return ret
        nearest = self.routing_table[channel].nearest(target)
        if not nearest or (use_local_storage and max(nearest) > distance(self.self_info.channels[channel].id, target)):
            ret.set_result(None)
            return ret
        selection = choices(
            tuple(nearest.values()),
            weights=[peer.local.score for peer in nearest.values()],
            k=subnet.alpha
        )
        msg = FindKeyMessage(target=target, key=key, channel=channel)
        msg.async_res = ret
        for peer in selection:
            self._send(self.socks[peer.local.sock], peer.local.addr, msg)
        return ret

    def set(self, key: bytes, value: Any, channel: int = 0):
        """Set a value in the distributed hash table.

        Raises
        ------
        - PermissionError: If DHT features are disabled on the requested channel
        """
        subnet = self.self_info.channels[channel].subnet
        if subnet.dht_disabled:
            raise PermissionError("DHT features are disabled on this channel.")
        target = subnet.h_func(key).digest()
        dist = distance(self.self_info.channels[channel].id, target)
        msg = StoreKeyMessage(target=target, key=key, value=value, channel=channel)
        self.storage[channel][key] = deepcopy(value)
        msg._schedule_val_expire(self, dist, originator=True)
        for peer in self.routing_table[channel].nearest(target).values():
            self.logger.debug("Sending a SET to %r", peer)
            self._send(self.socks[peer.local.sock], peer.local.addr, msg)
