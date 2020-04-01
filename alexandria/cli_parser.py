import argparse

from alexandria import __version__


parser = argparse.ArgumentParser(description='Alexandria')

#
# subparser for sub commands
#
# Components may add subcommands with a `func` attribute
# to gain control over the main Trinity process
subparser = parser.add_subparsers(dest='subcommand')

#
# Argument Groups
#
alexandria_parser = parser.add_argument_group('core')
logging_parser = parser.add_argument_group('logging')
network_parser = parser.add_argument_group('network')


#
# Trinity Globals
#
alexandria_parser.add_argument('--version', action='version', version=__version__)
alexandria_parser.add_argument('-p', '--private-key-seed', type=str, dest='private_key_seed')
alexandria_parser.add_argument(
    '-b', '--bootnodes',
    type=str,
    action='append',
    dest='bootnodes',
)

#
# Logging configuration
#
logging_parser.add_argument(
    '-l',
    '--log-level',
    type=str,
    dest="log_level",
    help=(
        "Configure the logging level. ",
    ),
)

#
# Parser for networking config
#
network_parser.add_argument(
    '--port',
    type=int,
    required=False,
    default=30314,
    help=(
        "Port to listen for incoming connections."
    ),
)
