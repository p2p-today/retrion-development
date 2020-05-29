from logging import basicConfig, DEBUG
from queue import SimpleQueue
from time import localtime
from tkinter import Entry, Frame, StringVar, Text, Tk, Toplevel, BOTTOM, INSERT, X
from typing import Optional, Sequence, Tuple

from ..kademlia_v05 import KademliaNode
from ..protocol.v05 import BroadcastMessage, Message, NetworkConfiguration


def main():
    """Start two windows and run their event loops."""
    basicConfig(level=DEBUG)
    root = Tk()
    make_window(44565, 'alice', root)
    make_window(44566, 'bob', root)
    root.mainloop()


class ChatNode(KademliaNode):
    """A small extension of the base node to hold metadata and an event queue."""

    def __init__(self, port: int, username: str, tk_root, queue: SimpleQueue):
        super().__init__(port)
        self.username = username
        self.tk_root = tk_root
        self.queue = queue


@Message.register(16)
class ChatMessage(BroadcastMessage):
    """A small extension of the BroadcastMessage object to trigger TK events."""

    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0,
        payload: Optional[str] = None
    ):
        super().__init__(compress, seq, sender, channel, payload)
        self.message_type = 128  # make sure it sets the correct message type or it will deserialize as BroadcastMessage

    def react(self, node, addr, sock):
        """Extend the inherited react() to add this to trigger a TK event."""
        if super().react(node, addr, sock):  # BroadcastMessage.react() returns True if it was a new message
            if not isinstance(self.payload, Sequence) or len(self.payload) != 2:
                node.logger.info("Got an invalid message. Ignoring.")
                return  # we don't do this outside the if because we want it to affect connections still
            node.queue.put(self)
            node.tk_root.event_generate("<<Message Received>>", when="tail")


def make_window(port, username, root):
    """Create a chat application window and its associated node."""
    # TK boilerplate
    window = Toplevel(root)
    messages = Text(window)
    messages.pack()
    input_user = StringVar()
    input_field = Entry(window, text=input_user)
    input_field.pack(side=BOTTOM, fill=X)

    # set up the message queue to deal with TKs thread pickyness
    queue = SimpleQueue()

    def message_received(event):                    # when we get a message
        message = queue.get()                       # grab it from the queue
        sender_id, input_get = message.payload      # grab the sender's name and message
        t = localtime(message.seq[0] / 1000)        # convert from Hybrid Logical Clock to local time
        messages.insert(                            # add to the chat log
            INSERT,
            '[%02i:%02i:%02i] %s: %s\n' % (t.tm_hour, t.tm_min, t.tm_sec, sender_id, input_get)
        )

    def enter_pressed(event):                                                       # when the user hits enter
        input_get = input_field.get()                                               # grab the message
        input_user.set('')                                                          # and clear the text box
        node.send_all(ChatMessage(channel=0, payload=(node.username, input_get)))   # broadcast it to everybody
        t = localtime(node.tick()[0] / 1000)                                        # grab the time and convert from HLC to local time
        messages.insert(                                                            # then add it to the chat log
            INSERT,
            '[%02i:%02i:%02i] %s: %s\n' % (t.tm_hour, t.tm_min, t.tm_sec, node.username, input_get)
        )
        return "break"

    # TK boilerplate
    frame = Frame(window)
    input_field.bind("<Return>", enter_pressed)
    input_field.bind("<<Message Received>>", message_received)
    frame.pack()

    node = ChatNode(port, username, input_field, queue)     # create a node
    node.register_channel(                                  # register the channel for your chat application
        name="Chat",
        description="A simple broadcast-based chat application",
        subnet=NetworkConfiguration(
            channel=0,          # channel must be agreed upon (for now)
            b=1,                # lookup accelleration param (trade off is routing table size)
            k=5,                # replication param (more connections, more DHT backups)
            h_name="MD5",       # dumb hash for a dumb app
            dht_disabled=True   # turn off the DHT features
        )
    )
    node.bootstrap(0)                                       # then ask the bootstrap network for peers
