# Retrion Development Area

This is a development codebase for the Retrion protocol.

## What is Retrion?

Retrion is a standardized extension of Kademlia that aims to provide an environment which makes peer-to-peer
applications significantly easier to develop. It seeks to provide the following:

1. An object model and serialization standard that can be used easily in most languages
2. A simple high-level API (along with a fine-grained one)
3. log(n) distributed hash table get/sets
4. Support for disabling DHT support network-wide
5. Support for "edge nodes" which do not store DHT data except as a cache
6. log(n) broadcast delivery time
7. log(n) routed message delivery
8. Support for multicast routed messages
9. Support for in-protocol group messages
10. Transparent (to the developer) compression
11. transparent (to the user) encryption
12. Support for multiple transport protocols (TCP, UDP, etc.)
13. Support for "virtual transports" (using other nodes as a bridge)
14. A well-defined standard configuration space
15. Support for extension subprotocols and implementation-specific configurations

Currently, this implementation meets 1-4, 6, 10, and 14-15. That makes 8/15 goals.

## How Do I Use This?

### Basic Concepts

You can make a node object with a given port, and it will automatically open listening sockets on said port and set up
some basic elements.

You then register a subnet on that node by calling `node.register_channel()`, providing it a `NetworkConfiguration`.
This set up a listening channel on the nodes internally multiplexer.

You then must `connect()` to at least one other node on the subnet. You can do this by either contacting a known seed
node, or you can do this by calling `bootstrap()` initially instead of `register_channel()`. This will have it contact
the (not currently live) "bootstrap network", hosting a DHT of `NetworkConfiguration`s to lists of nodes that are or
have been on the network.

Beyond basic peerfinding and maintanence, you are required to extend the protocol to suit your application. You do this
by inheriting from the `Message` object and calling `Message.register()` on a resulting subclass. All messages have a
method called `react()` which allows them to parse and respond to messages, reconfiguring themselves as needed. Some
messages also have a `react_response()` method that allows you to parse a response to said message.

On subnets that enable this feature, you can use the `get()` and `set()` methods to modify shared global data which
each node stores a portion of. Do note that any keys you set will expire if your node leaves the network, and that this
storage does not guarantee causal consistency, only eventual consistency.

### Comparison to SocketIO

`Message.react()` is analagous to `io.Socket.on()`. `Message.react_response()` is analagous to `io.Socket.emit()`'s
`ack` argument.

### Examples

Provided is an example chat app. When the bootstrap network is active this will provide you two chat instances that
find each other without hardcoded addresses or formulas. You can use this to show that Alice and Bob can talk to each
other in an IRC-like way.

# Current Work

Currently I'm working on improving the efficiency of broadcast messages and trying to get a group messaging API in
place, or at least allow for multicast messages.

# Future Work

At some point I'll need to tackle encryption, but I'm really reluctant to mess with that on my own, since I'm fairly
inexperienced with cryptography or security in general.

Probably the next batch of work will be on using multiple transport methods. I need some extra time to figure out what
the implications of having nodes on a network that cannot directly connect with each other, and potential solutions
around that. Additionally, some work needs to be done to ensure that this protocol can work well over a streaming API
instead of a datagram one. Connection management presents a problem as well, though one that can probably be solved via
a locally configured connection limit that defaults to something in the couple-hundreds.

# Wild Brainstorming

Seems like you could implement a SQLite engine on the k closest nodes that would always agree with each other, using
only mild tweaking to the message send algorithm. Each transaction would be processed in order of the COMMIT's HLC
timestamp with ties going to the node closest to the table name in question. There are additional race conditions that
might occur though, and I don't know how to prevent them quite yet.
