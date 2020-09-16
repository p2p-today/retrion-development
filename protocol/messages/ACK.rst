N/ACK
#####

Version 7
=========

Message Type Code
+++++++++++++++++

The type code of this message is 0.

Payload Description
+++++++++++++++++++

The payload of this message is always a tuple composed of a 3-tuple representing the message identifier this is a
reply to, an optional integer representing the return status, and an optional return payload. If the payload is
included, it must also include a return status.

Message Identifier
~~~~~~~~~~~~~~~~~~

This field contains a 3-tuple of the originating message's timestamp, nonce, and sender ID. This section should be
unique for each message with very low likelihood of non-engineered overlaps, restricted further by constraints on valid
timestamp-nonce pairs.

Return Status
~~~~~~~~~~~~~

As in Unix, this should always be 0 if the message is successful. If so, the return message can be compressed more
than otherwise.

Return Payload
~~~~~~~~~~~~~~

As in Python, this is None by default. Otherwise its contents can vary based on the return code. For instance, a return
code of 1 may indicate that this payload is a different format than a return code of 0.

Restricted Routing Strategies
+++++++++++++++++++++++++++++

ACKs are a highly discouraged message on any type of broadcast strategy. It is not technically a violation of the
protocol, but it is highly likely to produce network congestion and should be avoided if at all possible.
