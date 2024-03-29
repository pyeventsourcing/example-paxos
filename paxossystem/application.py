from typing import Any, Mapping, Optional, Type
from uuid import UUID

from eventsourcing.application import AggregateNotFound, ProcessingEvent
from eventsourcing.domain import AggregateEvent, TAggregate
from eventsourcing.persistence import Transcoder
from eventsourcing.system import ProcessApplication
from eventsourcing.utils import get_topic

from paxossystem.composable import PaxosMessage
from paxossystem.domainmodel import PaxosAggregate
from paxossystem.transcodings import (
    AcceptAsDict,
    AcceptedAsDict,
    NackAsDict,
    PrepareAsDict,
    PromiseAsDict,
    ProposalIDAsDict,
    ProposalStatusAsDict,
    ResolutionAsDict,
    SetAsList,
)


class PaxosApplication(ProcessApplication):
    num_participants: int = 1
    notify_topics = [
        get_topic(PaxosAggregate.MessageAnnounced),
    ]

    def __init__(self, env: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(env)
        self.assume_leader = False
        self.announce_nacks = False
        self.announce_resolutions = False

    def register_transcodings(self, transcoder: Transcoder) -> None:
        super().register_transcodings(transcoder)
        transcoder.register(ProposalIDAsDict())
        transcoder.register(PrepareAsDict())
        transcoder.register(PromiseAsDict())
        transcoder.register(AcceptAsDict())
        transcoder.register(AcceptedAsDict())
        transcoder.register(NackAsDict())
        transcoder.register(ResolutionAsDict())
        transcoder.register(SetAsList())
        transcoder.register(ProposalStatusAsDict())

    def propose_value(self, key: UUID, value: Any) -> PaxosAggregate:
        """
        Starts new Paxos aggregate and proposes a value for a key.
        """
        paxos_aggregate = self.start_paxos(key, value, self.assume_leader)
        self.save(paxos_aggregate)
        return paxos_aggregate  # in case it's new

    def start_paxos(self, key, value, assume_leader=False, paxos_cls=PaxosAggregate):
        assert self.num_participants > 1
        assert isinstance(key, UUID)
        quorum_size = 1 + self.num_participants // 2
        paxos_aggregate = paxos_cls.start(
            originator_id=key,
            quorum_size=quorum_size,
            network_uid=self.name,
            announce_nacks=self.announce_nacks,
            announce_resolution=self.announce_resolutions,
        )
        paxos_aggregate.propose_value(value, assume_leader=assume_leader)
        return paxos_aggregate

    def get_final_value(self, key: UUID) -> PaxosAggregate:
        return self.repository.get(key).final_value

    def policy(
        self,
        domain_event: AggregateEvent[TAggregate],
        process_event: ProcessingEvent,
    ) -> None:
        """
        Processes paxos "message announced" events of other applications
        by starting or continuing a paxos aggregate in this application.
        """
        if isinstance(domain_event, PaxosAggregate.MessageAnnounced):
            paxos, resolution_msg = self.process_announced_message(domain_event)
            process_event.collect_events(paxos)

    def process_announced_message(
        self,
        domain_event: PaxosAggregate.MessageAnnounced,
        paxos_cls: Type[PaxosAggregate] = PaxosAggregate,
    ):
        # Get or create aggregate.
        try:
            paxos_aggregate = self.repository.get(domain_event.originator_id)
        except AggregateNotFound:
            quorum_size = 1 + self.num_participants // 2
            paxos_aggregate = paxos_cls.start(
                originator_id=domain_event.originator_id,
                quorum_size=quorum_size,
                network_uid=self.name,
                announce_nacks=self.announce_nacks,
                announce_resolution=self.announce_resolutions,
            )
        # Absolutely make sure the participant aggregates aren't getting confused.
        assert (
            paxos_aggregate.network_uid == self.name
        ), "Wrong paxos aggregate: required network_uid {}, got {}".format(
            self.name, paxos_aggregate.network_uid
        )
        # Only receive messages until resolution is
        # obtained. Followers will process our previous
        # announcements and resolve to the same final value
        # before processing anything we could announce after.
        msg = domain_event.msg
        assert isinstance(msg, PaxosMessage)
        resolution_msg = None
        if paxos_aggregate.final_value is None:
            resolution_msg = paxos_aggregate.receive_message(msg)
        return paxos_aggregate, resolution_msg
