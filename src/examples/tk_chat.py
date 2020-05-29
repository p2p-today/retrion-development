# stupidly, you need to paste this into a console to make it work. will debug later

from logging import basicConfig, DEBUG
from queue import SimpleQueue
from time import localtime
from tkinter import Entry, Frame, StringVar, Text, Tk, Toplevel, BOTTOM, INSERT, X
from typing import Optional, Sequence, Tuple

from ..kademlia_v05 import KademliaNode
from ..protocol.v05 import BroadcastMessage, Message, NetworkConfiguration


class ChatNode(KademliaNode):
    def __init__(self, port, username, tk_root, queue: SimpleQueue):
        super().__init__(port)
        self.username = username
        self.tk_root = tk_root
        self.queue = queue


@Message.register(16)
class ChatMessage(BroadcastMessage):
    def __init__(
        self,
        compress: int = 0,
        seq: Optional[Tuple[int, int]] = None,
        sender: bytes = b'',
        channel: int = 0,
        payload: Optional[str] = None
    ):
        super().__init__(compress, seq, sender, channel, payload)
        self.message_type = 16

    def react(self, node, addr, sock):
        if super().react(node, addr, sock):
            if not isinstance(self.payload, Sequence) or len(self.payload) != 2:
                node.logger.info("Got an invalid message. Ignoring.")
                return
            node.queue.put(self)
            node.tk_root.event_generate("<<Message Received>>", when="tail")


def make_window(port, username, root):
    queue = SimpleQueue()
    window = Toplevel(root)
    messages = Text(window)
    messages.pack()
    input_user = StringVar()
    input_field = Entry(window, text=input_user)
    input_field.pack(side=BOTTOM, fill=X)

    def message_received(event):
        message = queue.get()
        sender_id, input_get = message.payload
        t = localtime(message.seq[0] / 1000)
        messages.insert(
            INSERT,
            '[%02i:%02i:%02i] %s: %s\n' % (t.tm_hour, t.tm_min, t.tm_sec, sender_id, input_get)
        )

    def enter_pressed(event):
        input_get = input_field.get()
        node.send_all(ChatMessage(channel=0, payload=(node.username, input_get)))
        t = localtime(node.tick()[0] / 1000)
        messages.insert(
            INSERT,
            '[%02i:%02i:%02i] %s: %s\n' % (t.tm_hour, t.tm_min, t.tm_sec, node.username, input_get)
        )
        input_user.set('')
        return "break"

    frame = Frame(window)
    input_field.bind("<Return>", enter_pressed)
    input_field.bind("<<Message Received>>", message_received)
    frame.pack()
    node = ChatNode(port, username, input_field, queue)
    node.register_channel(
        name="Chat",
        description="A simple broadcast-based chat application",
        subnet=NetworkConfiguration(channel=0, b=1, k=5, h_name="MD5", dht_disabled=True)
    )
    node.bootstrap(0)


def main():
    basicConfig(level=DEBUG)
    root = Tk()
    make_window(44565, 'alice', root)
    make_window(44566, 'bob', root)
    root.mainloop()
