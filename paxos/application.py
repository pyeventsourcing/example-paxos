from typing import Any
from uuid import UUID

from eventsourcing.application import AggregateNotFound, ProcessEvent
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import Transcoder
from eventsourcing.system import ProcessApplication

from paxos.composable import (
    PaxosMessage,
)
from paxos.domainmodel import PaxosAggregate
from paxos.transcodings import AcceptAsDict, AcceptedAsDict, NackAsDict, PrepareAsDict, PromiseAsDict, \
    ProposalIDAsDict, ProposalStatusAsDict, ResolutionAsDict, SetAsList


class PaxosApplication(ProcessApplication[Aggregate]):
    quorum_size: int = 0
    notification_log_section_size = 5
    use_cache = True
    set_notification_ids = True

    snapshotting_intervals = {
        # PaxosAggregate: 3
    }

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

        Decorated with retry in case of notification log conflict
        or operational error.

        This a good example of writing process event from an
        application command. Just get the batch of pending events
        and record a process event with those events alone. They
        will be written atomically.
        """
        assert self.quorum_size > 0
        assert isinstance(key, UUID)
        paxos_aggregate = PaxosAggregate.start(
            originator_id=key, quorum_size=self.quorum_size, network_uid=self.name
        )
        msg = paxos_aggregate.propose_value(value, assume_leader=assume_leader)
        paxos_aggregate.receive_message(msg)

        self.save(paxos_aggregate)

        return paxos_aggregate  # in case it's new

    def get_final_value(self, key: UUID) -> PaxosAggregate:
        return self.repository.get(key).final_value

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
            paxos = self.repository.get(domain_event.originator_id)
        except AggregateNotFound:
            paxos = PaxosAggregate.start(
                originator_id=domain_event.originator_id,
                quorum_size=self.quorum_size,
                network_uid=self.name,
            )
            # # Needs to go in the cache now, otherwise we get
            # # "Duplicate" errors (for some unknown reason).
            # if self.repository.use_cache:
            #     self.repository.put_entity_in_cache(paxos.id, paxos)
        # Absolutely make sure the participant aggregates aren't getting confused.
        assert (
            paxos.network_uid == self.name
        ), "Wrong paxos aggregate: required network_uid {}, got {}".format(
            self.name, paxos.network_uid
        )
        # Only receive messages until resolution is
        # obtained. Followers will process our previous
        # announcements and resolve to the same final value
        # before processing anything we could announce after.
        msg = domain_event.msg
        assert isinstance(msg, PaxosMessage)
        resolution_msg = None
        if paxos.final_value is None:
            resolution_msg = paxos.receive_message(msg)
        # if resolution_msg:
        #     print("Final value:", paxos.final_value)
        return paxos, resolution_msg


