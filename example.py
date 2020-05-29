import argparse
from random import choice


def main():
    """Choose an example."""
    parser = argparse.ArgumentParser('Run an example Retrion program')
    parser.add_argument(
        '--tk_chat',
        action='store_true',
        help='Run the TK chat example, starting two chat instances that find each other over the bootstrap network'
    )
    parser.add_argument(
        '--random',
        action='store_true',
        help='Run a random example program'
    )

    args = parser.parse_args()

    if args.random:
        choice((tk_chat, ))()
    if args.tk_chat:
        tk_chat()


def tk_chat():
    """Start the TK chat example."""
    from src.examples import tk_chat
    tk_chat.main()


if __name__ == '__main__':
    main()
