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

### Comparison to SocketIO

`Message.react()` is analagous to `io.Socket.on()`. `Message.react_response()` is analagous to `io.Socket.emit()`'s
`ack` argument.

### Examples

Provided is an example chat app. When the bootstrap network is active this will provide you two chat instances that
find each other without hardcoded addresses or formulas. You can use this to show that Alice and Bob can talk to each
other in an IRC-like way.
