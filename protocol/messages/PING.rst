PING
####

Version 7
=========

Message Type Code
+++++++++++++++++

The type code of this message is 1.

Payload Description
+++++++++++++++++++

There is no payload in this message

Expected Reaction
+++++++++++++++++

A PING should always be replied to with an ACK

Restricted Routing Strategies
+++++++++++++++++++++++++++++

PING is a disallowed message on any type of broadcast strategy. It is explicitly a violation of the protocol for it to
appear in the TREE and FLOOD strategies.
