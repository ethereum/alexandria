from argparse import Namespace
import logging
import time
from types import ModuleType
from typing import Any, Dict, Type

from async_service import Service
import ssz
from pyformance import MetricsRegistry
from pyformance.reporters import InfluxReporter

from alexandria._utils import every
from alexandria.abc import ClientAPI, EventAPI, KademliaAPI
from alexandria.payloads import (
    Advertise, Ack,
    Locate, Locations,
    Retrieve, Chunk,
    Ping, Pong,
    FindNodes, FoundNodes,
)
from alexandria.system_metrics import (
    read_cpu_stats,
    read_disk_stats,
    read_network_stats,
    SystemStats,
)


PAYLOAD_TYPES = (
    Advertise, Ack,
    Locate, Locations,
    Retrieve, Chunk,
    Ping, Pong,
    FindNodes, FoundNodes,
)


class HostMetricsRegistry(MetricsRegistry):  # type: ignore
    def __init__(self, host: str, clock: ModuleType = time) -> None:
        super().__init__(clock)
        self.host = host

    def dump_metrics(self) -> Dict[str, Dict[str, Any]]:
        metrics = super().dump_metrics()

        for key in metrics:
            # We want every metric to include a 'host' identifier to be able to filter accordingly
            metrics[key]['host'] = self.host

        return metrics  # type: ignore


class Metrics(Service):
    logger = logging.getLogger('alexandria.metrics.Metrics')

    def __init__(self,
                 host: str,
                 client: ClientAPI,
                 kademlia: KademliaAPI,
                 influx_server: str,
                 influx_user: str,
                 influx_password: str,
                 influx_database: str,
                 influx_port: int = 443,
                 influx_protocol: str = 'https',
                 reporting_frequency: int = 10,
                 process_collection_frequency: int = 3):
        self._influx_server = influx_server

        self._reporting_frequency = reporting_frequency
        self._process_collection_frequency = process_collection_frequency

        self._registry = HostMetricsRegistry(host)

        self._reporter = InfluxReporter(
            registry=self._registry,
            protocol=influx_protocol,
            port=influx_port,
            database=influx_database,
            username=influx_user,
            password=influx_password,
            server=influx_server
        )

        self.client = client
        self.kademlia = kademlia

    @classmethod
    def from_cli_args(cls,
                      args: Namespace,
                      client: ClientAPI,
                      kademlia: KademliaAPI,
                      ) -> 'Metrics':
        return cls(
            host=args.metrics_host,
            client=client,
            kademlia=kademlia,
            influx_server=args.metrics_influx_server,
            influx_user=args.metrics_influx_user,
            influx_password=args.metrics_influx_password,
            influx_database=args.metrics_influx_database,
            influx_port=args.metrics_influx_port,
            influx_protocol=args.metrics_influx_protocol,
            reporting_frequency=args.metrics_reporting_frequency,
        )

    async def run(self) -> None:
        self.manager.run_daemon_task(
            self._continuously_report,
            self._reporting_frequency,
        )
        self.manager.run_daemon_task(
            self._collect_system_metrics,
            self._process_collection_frequency,
        )
        self.manager.run_daemon_task(
            self._report_routing_table_stats,
            10,
        )
        self.manager.run_daemon_task(
            self._report_content_manager_stats,
            10,
        )
        self.logger.info('Metrics started')
        for payload_type in PAYLOAD_TYPES:
            self.manager.run_daemon_task(self._report_inbound_message_stats, payload_type)

        self.manager.run_daemon_task(self._report_event, self.client.events.session_created, 'events/session-created')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.session_idle, 'events/session-idle')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.handshake_complete, 'events/handshake-complete')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.handshake_timeout, 'events/handshake-timeout')  # noqa: E501

        self.manager.run_daemon_task(self._report_event, self.client.events.sent_ping, 'messages/outbound/Ping')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_pong, 'messages/outbound/Pong')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_find_nodes, 'messages/outbound/FindNodes')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_found_nodes, 'messages/outbound/FoundNodes')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_advertise, 'messages/outbound/Advertise')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_ack, 'messages/outbound/Ack')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_locate, 'messages/outbound/Locate')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_locations, 'messages/outbound/Locations')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_retrieve, 'messages/outbound/Retrieve')  # noqa: E501
        self.manager.run_daemon_task(self._report_event, self.client.events.sent_chunk, 'messages/outbound/Chunk')  # noqa: E501

        await self.manager.wait_finished()

    async def _continuously_report(self, frequency: int) -> None:
        async for _ in every(frequency):
            self._reporter.report_now()

    async def _report_event(self, event: EventAPI[Any], suffix: str) -> None:
        counter = self._registry.counter(f'alexandria.{suffix}.counter')
        meter = self._registry.meter(f'alexandria.{suffix}.meter')

        async with event.subscribe() as subscription:
            async for _ in subscription:
                counter.inc()
                meter.mark()

    async def _report_routing_table_stats(self, frequency: int) -> None:
        size_gauge = self._registry.gauge('alexandria.dht/routing-table/total-nodes.gauge')
        async for _ in every(frequency):
            stats = self.kademlia.routing_table.get_stats()
            size_gauge.set_value(stats.total_nodes)
            self.logger.debug('Reported routing table metrics')

    async def _report_inbound_message_stats(self, payload_type: Type[ssz.Serializable]) -> None:
        name = payload_type.__name__
        counter = self._registry.counter(f'alexandria.messages/inbound/{name}.counter')
        meter = self._registry.meter(f'alexandria.messages/inbound/{name}.meter')

        async with self.client.message_dispatcher.subscribe(payload_type) as subscription:
            async for payload in subscription:
                counter.inc()
                meter.mark()

    async def _report_content_manager_stats(self, frequency: int) -> None:
        gauge = self._registry.gauge

        durable_db_item_count_gauge = gauge('alexandria.content/durable-db/item-count.gauge')

        ephemeral_db_item_count_gauge = gauge('alexandria.content/ephemeral-db/item-count.gauge')
        ephemeral_db_capacity_gauge = gauge('alexandria.content/ephemeral-db/capacity.gauge')
        ephemeral_db_size_gauge = gauge('alexandria.content/ephemeral-db/size.gauge')

        ephemeral_index_capacity_gauge = gauge('alexandria.content/ephemeral-index/capacity.gauge')
        ephemeral_index_size_gauge = gauge('alexandria.content/ephemeral-index/size.gauge')

        cache_db_item_count_gauge = gauge('alexandria.content/cache-db/item-count.gauge')
        cache_db_capacity_gauge = gauge('alexandria.content/cache-db/capacity.gauge')
        cache_db_size_gauge = gauge('alexandria.content/cache-db/size.gauge')

        cache_index_capacity_gauge = gauge('alexandria.content/cache-index/capacity.gauge')
        cache_index_size_gauge = gauge('alexandria.content/cache-index/size.gauge')

        async for _ in every(frequency):
            stats = self.kademlia.content_manager.get_stats()

            durable_db_item_count_gauge.set_value(stats.durable_item_count)

            ephemeral_db_item_count_gauge.set_value(stats.ephemeral_db_count)
            ephemeral_db_capacity_gauge.set_value(stats.ephemeral_db_capacity)
            ephemeral_db_size_gauge.set_value(
                stats.ephemeral_db_total_capacity - stats.ephemeral_db_capacity
            )

            ephemeral_index_capacity_gauge.set_value(stats.ephemeral_index_capacity)
            ephemeral_index_size_gauge.set_value(
                stats.ephemeral_index_total_capacity - stats.ephemeral_index_capacity
            )

            cache_db_item_count_gauge.set_value(stats.cache_db_count)
            cache_db_capacity_gauge.set_value(stats.cache_db_capacity)
            cache_db_size_gauge.set_value(stats.cache_db_total_capacity - stats.cache_db_capacity)

            cache_index_capacity_gauge.set_value(stats.cache_index_capacity)
            cache_index_size_gauge.set_value(
                stats.cache_index_total_capacity - stats.cache_index_capacity
            )
            self.logger.debug('Reported content metrics')

    async def _collect_system_metrics(self, frequency: int) -> None:
        cpu_sysload_gauge = self._registry.gauge('alexandria.system/cpu/sysload.gauge')
        cpu_syswait_gauge = self._registry.gauge('alexandria.system/cpu/syswait.gauge')

        disk_readdata_meter = self._registry.meter('alexandria.system/disk/readdata.meter')
        disk_writedata_meter = self._registry.meter('alexandria.system/disk/writedata.meter')

        network_in_packets_meter = self._registry.meter('alexandria.network/in/packets/total.meter')
        network_out_packets_meter = self._registry.meter('alexandria.network/out/packets/total.meter')  # noqa: E501

        previous = SystemStats(
            cpu_stats=read_cpu_stats(),
            disk_stats=read_disk_stats(),
            network_stats=read_network_stats(),
        )
        async for _ in every(frequency, initial_delay=frequency):
            current = SystemStats(
                cpu_stats=read_cpu_stats(),
                disk_stats=read_disk_stats(),
                network_stats=read_network_stats(),
            )

            global_time = current.cpu_stats.global_time - previous.cpu_stats.global_time
            cpu_sysload_gauge.set_value(global_time / frequency)
            global_wait = current.cpu_stats.global_wait_io - previous.cpu_stats.global_wait_io
            cpu_syswait_gauge.set_value(global_wait / frequency)

            read_bytes = current.disk_stats.read_bytes - previous.disk_stats.read_bytes
            disk_readdata_meter.mark(read_bytes)

            write_bytes = current.disk_stats.write_bytes - previous.disk_stats.write_bytes
            disk_writedata_meter.mark(write_bytes)

            in_packets = current.network_stats.in_packets - previous.network_stats.in_packets
            network_in_packets_meter.mark(in_packets)
            out_packets = current.network_stats.out_packets - previous.network_stats.out_packets
            network_out_packets_meter.mark(out_packets)

            previous = current
            self.logger.debug('Reported system metrics')
