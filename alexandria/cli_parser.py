import argparse
import os

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
    type=int,
    dest="log_level",
    help=(
        "Configure the logging level. "
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


#
# Metrics
#
metrics_parser = parser.add_argument_group('metrics')

metrics_parser.add_argument(
    "--enable-metrics",
    action="store_true",
    help="Enable metrics component",
)

metrics_parser.add_argument(
    '--metrics-host',
    help='Host name to tag the metrics data (e.g. alexandria-bootnode-aws-west)',
    default=os.environ.get('ALEXANDRIA_METRICS_HOST'),
)

metrics_parser.add_argument(
    '--metrics-influx-user',
    help='Influx DB user. Defaults to `alexandria`',
    default=os.environ.get('ALEXANDRIA_METRICS_INFLUX_DB_USER', 'alexandria'),
)

metrics_parser.add_argument(
    '--metrics-influx-database',
    help='Influx DB name. Defaults to `alexandria`',
    default=os.environ.get('ALEXANDRIA_METRICS_INFLUX_DB_NAME', 'alexandria'),
)

metrics_parser.add_argument(
    '--metrics-influx-password',
    help='Influx DB password. Defaults to ENV var ALEXANDRIA_METRICS_INFLUX_DB_PASSWORD',
    default=os.environ.get('ALEXANDRIA_METRICS_INFLUX_DB_PASSWORD'),
)

metrics_parser.add_argument(
    '--metrics-influx-server',
    help='Influx DB server. Defaults to ENV var ALEXANDRIA_METRICS_INFLUX_DB_SERVER',
    default=os.environ.get('ALEXANDRIA_METRICS_INFLUX_DB_SERVER'),
)

metrics_parser.add_argument(
    '--metrics-influx-port',
    help='Influx DB port. Defaults to 443',
    type=int,
    default=int(os.environ.get('ALEXANDRIA_METRICS_INFLUX_DB_PORT', 443)),
)

metrics_parser.add_argument(
    '--metrics-influx-protocol',
    help='Influx DB protocol. Defaults to `https`',
    default=os.environ.get('ALEXANDRIA_METRICS_INFLUX_DB_PROTOCOL', 'https'),
)

metrics_parser.add_argument(
    '--metrics-reporting-frequency',
    help='The frequency in seconds at which metrics are reported',
    type=int,
    default=int(os.environ.get('ALEXANDRIA_METRICS_REPORTING_FREQUENCY', 10)),
)

metrics_parser.add_argument(
    '--metrics-system-collector-frequency',
    help='The frequency in seconds at which system metrics are collected',
    type=int,
    default=int(os.environ.get('ALEXANDRIA_METRICS_SYSTEM_COLLECTOR_FREQUENCY', 3)),
)
