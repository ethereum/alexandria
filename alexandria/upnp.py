import ipaddress
import logging
import netifaces
from typing import Tuple
from urllib.parse import urlparse

import upnpclient


class PortMapFailed(Exception):
    pass


class WANServiceNotFound(Exception):
    pass


class NoInternalAddressMatchesDevice(Exception):
    pass


# UPnP discovery can take a long time, so use a loooong timeout here.
UPNP_DISCOVER_TIMEOUT_SECONDS = 30
UPNP_PORTMAP_DURATION = 30 * 60  # 30 minutes


logger = logging.getLogger('alexandria.upnp')


def setup_port_map(port: int, duration: int = UPNP_PORTMAP_DURATION) -> Tuple[str, str]:
    """
    Set up the port mapping

    :return: the IP address of the new mapping (or None if failed)
    """
    devices = upnpclient.discover()
    if not devices:
        raise PortMapFailed("No UPnP devices available")

    for upnp_dev in devices:
        try:
            internal_ip, external_ip = setup_device_port_map(upnp_dev, port, duration)
            logger.info(
                "NAT port forwarding successfully set up: internal=%s:%d external=%s:%d",
                internal_ip, port,
                external_ip, port,
            )
            break
        except NoInternalAddressMatchesDevice:
            logger.debug(
                "No internal addresses were managed by the UPnP device at %s",
                upnp_dev.location,
            )
            continue
        except WANServiceNotFound:
            logger.debug(
                "No WAN services managed by the UPnP device at %s",
                upnp_dev.location,
            )
            continue
        except PortMapFailed:
            logger.debug(
                "Failed to setup portmap on UPnP divec at %s",
                upnp_dev.location,
                exc_info=True,
            )
            continue
    else:
        logger.info("Failed to setup NAT portmap.  Tried %d devices", len(devices))
        raise PortMapFailed(f"Failed to setup NAT portmap.  Tried {len(devices)} devices.")

    return internal_ip, external_ip


def find_internal_ip_on_device_network(upnp_dev: upnpclient.upnp.Device) -> str:
    """
    For a given UPnP device, return the internal IP address of this host machine that can
    be used for a NAT mapping.
    """
    parsed_url = urlparse(upnp_dev.location)
    # Get an ipaddress.IPv4Network instance for the upnp device's network.
    upnp_dev_net = ipaddress.ip_network(parsed_url.hostname + '/24', strict=False)
    for iface in netifaces.interfaces():
        for family, addresses in netifaces.ifaddresses(iface).items():
            # TODO: Support IPv6 addresses as well.
            if family != netifaces.AF_INET:
                continue
            for item in addresses:
                if ipaddress.ip_address(item['addr']) in upnp_dev_net:
                    return str(item['addr'])
    raise NoInternalAddressMatchesDevice(parsed_url.hostname)


WAN_SERVICE_NAMES = (
    'WANIPConn1',
    'WANIPConnection.1',  # Nighthawk C7800
)


def get_wan_service(upnp_dev: upnpclient.upnp.Device) -> upnpclient.upnp.Service:
    for service_name in WAN_SERVICE_NAMES:
        try:
            return upnp_dev[service_name]
        except KeyError:
            continue
    else:
        raise WANServiceNotFound()


def setup_device_port_map(upnp_dev: upnpclient.upnp.Device,
                          port: int,
                          duration: int) -> Tuple[str, str]:
    internal_ip = find_internal_ip_on_device_network(upnp_dev)
    wan_service = get_wan_service(upnp_dev)

    external_ip = wan_service.GetExternalIPAddress()['NewExternalIPAddress']

    try:
        wan_service.AddPortMapping(
            NewRemoteHost=external_ip,
            NewExternalPort=port,
            NewProtocol='UDP',
            NewInternalPort=port,
            NewInternalClient=internal_ip,
            NewEnabled='1',
            NewPortMappingDescription='alexandria',
            NewLeaseDuration=duration,
        )
    except upnpclient.soap.SOAPError as exc:
        if exc.args == (718, 'ConflictInMappingEntry'):
            # An entry already exists with the parameters we specified. Maybe the router
            # didn't clean it up after it expired or it has been configured by other piece
            # of software, either way we should not override it.
            # https://tools.ietf.org/id/draft-ietf-pcp-upnp-igd-interworking-07.html#errors
            logger.debug("NAT port mapping already configured, not overriding it")
            return internal_ip, external_ip
        else:
            logger.debug(
                "Failed to setup NAT portmap on device: %s",
                upnp_dev.location,
            )
            raise PortMapFailed from exc
    else:
        return internal_ip, external_ip
