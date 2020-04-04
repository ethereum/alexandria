ALEXANDRIA_HEADER = "\n".join((
    "",
    r"           _                          _      _       ",
    r"     /\   | |                        | |    (_)      ",
    r"    /  \  | | _____  ____ _ _ __   __| |_ __ _  __ _ ",
    r"   / /\ \ | |/ _ \ \/ / _` | '_ \ / _` | '__| |/ _` |",
    r"  / ____ \| |  __/>  < (_| | | | | (_| | |  | | (_| |",
    r" /_/    \_\_|\___/_/\_\__,_|_| |_|\__,_|_|  |_|\__,_|",
    "",
))


async def main() -> None:
    import ipaddress
    import logging
    import os
    import secrets

    from async_service import background_trio_service

    from alexandria._utils import sha256
    from alexandria.abc import Endpoint
    from alexandria.application import Application
    from alexandria.cli_parser import parser
    from alexandria.enode import ENode
    from alexandria.logging import setup_logging

    from eth_keys import keys

    DEFAULT_LISTEN_ON = Endpoint(ipaddress.IPv4Address('0.0.0.0'), 8628)

    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.port is not None:
        listen_on = Endpoint(ipaddress.IPv4Address('0.0.0.0'), args.port)
    else:
        listen_on = DEFAULT_LISTEN_ON

    logger = logging.getLogger()

    if args.private_key_seed is None:
        private_key = keys.PrivateKey(secrets.token_bytes(32))
    else:
        private_key = keys.PrivateKey(sha256(args.private_key_seed))

    bootnodes_raw = args.bootnodes or ()
    bootnodes = tuple(
        ENode.from_enode_uri(enode).node for enode in bootnodes_raw
    )

    application = Application(
        bootnodes=bootnodes,
        private_key=private_key,
        listen_on=listen_on,
    )

    logger.info(ALEXANDRIA_HEADER)
    logger.info("Started main process (pid=%d)", os.getpid())
    async with background_trio_service(application) as manager:
        await manager.wait_finished()
