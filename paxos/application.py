from typing import Any
from uuid import UUID

from eventsourcing.application import AggregateNotFound, ProcessEvent
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import Transcoder
from eventsourcing.system import ProcessApplication
from eventsourcing.utils import get_topic

from paxos.cache import CachingApplication
from paxos.composable import PaxosMessage
from paxos.domainmodel import PaxosAggregate
from paxos.transcodings import (
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


class PaxosApplication(CachingApplication[Aggregate], ProcessApplication[Aggregate]):
    quorum_size: int = 0
    follow_topics = [
        get_topic(PaxosAggregate.MessageAnnounced),
    ]

    @property
    def name(self):
        return self.__class__.__name__

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

    def propose_value(
        self, key: UUID, value: Any, assume_leader: bool = False
    ) -> PaxosAggregate:
        """
        Starts new Paxos aggregate and proposes a value for a key.
        """
        paxos_aggregate = self.start_paxos(key, value, assume_leader)
        self.save(paxos_aggregate)
        return paxos_aggregate  # in case it's new

    def start_paxos(self, key, value, assume_leader):
        assert self.quorum_size > 0
        assert isinstance(key, UUID)
        paxos_aggregate = PaxosAggregate.start(
            originator_id=key, quorum_size=self.quorum_size, network_uid=self.name
        )
        msg = paxos_aggregate.propose_value(value, assume_leader=assume_leader)
        paxos_aggregate.receive_message(msg)
        return paxos_aggregate

    def get_final_value(self, key: UUID) -> PaxosAggregate:
        return self.repository.get(
            key,
        ).final_value

    def policy(
        self,
        domain_event: AggregateEvent[TAggregate],
        process_event: ProcessEvent,
    ) -> None:
        """
        Processes paxos "message announced" events of other applications
        by starting or continuing a paxos aggregate in this application.
        """
        if isinstance(domain_event, PaxosAggregate.MessageAnnounced):
            paxos, resolution_msg = self.process_message_announced(domain_event)
            process_event.save(paxos)

    def process_message_announced(self, domain_event):
        # Get or create aggregate.
        try:
            paxos_aggregate = self.repository.get(domain_event.originator_id)
        except AggregateNotFound:
            paxos_aggregate = PaxosAggregate.start(
                originator_id=domain_event.originator_id,
                quorum_size=self.quorum_size,
                network_uid=self.name,
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
