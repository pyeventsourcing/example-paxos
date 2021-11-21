import itertools
from typing import Any, Dict, List
from uuid import UUID

from eventsourcing.application import AggregateNotFound, ProcessEvent
from eventsourcing.domain import AggregateEvent, TAggregate
from eventsourcing.persistence import PersistenceError, Transcoder, Transcoding
from eventsourcing.system import ProcessApplication, System
from eventsourcing.utils import retry

from paxos.composable import Accept, Accepted, Nack, PaxosMessage, Prepare, Promise, ProposalID, ProposalStatus, \
    Resolution
from paxos.domainmodel import PaxosAggregate


class ObjAsDict(Transcoding):
    def encode(self, obj: object) -> Dict:
        return obj.__dict__

    def decode(self, data: Dict) -> object:
        return self.type(**data)


class ProposalIDAsDict(ObjAsDict):
    name = 'proposal_id'
    type = ProposalID


class PrepareAsDict(ObjAsDict):
    name = 'prepare'
    type = Prepare


class PromiseAsDict(ObjAsDict):
    name = 'promise'
    type = Promise


class AcceptAsDict(ObjAsDict):
    name = 'accept'
    type = Accept


class AcceptedAsDict(ObjAsDict):
    name = 'accepted'
    type = Accepted


class NackAsDict(ObjAsDict):
    name = 'nack'
    type = Nack


class ResolutionAsDict(ObjAsDict):
    name = 'resolution'
    type = Resolution


class ProposalStatusAsDict(Transcoding):
    name = 'proposal_status'
    type = ProposalStatus

    def encode(self, obj: Any) -> Any:
        return {
            "accept_count": obj.accept_count,
            "retain_count": obj.retain_count,
            "acceptors": obj.acceptors,
            "value": obj.value,
        }

    def decode(self, data: Any) -> Any:
        obj = ProposalStatus(data["value"])
        obj.accept_count = data["accept_count"]
        obj.retain_count = data["retain_count"]
        obj.acceptors = data["acceptors"]
        return obj


class SetAsList(Transcoding):
    name = 'set'
    type = set

    def encode(self, obj: Any) -> Any:
        return list(obj)

    def decode(self, data: Any) -> Any:
        return set(data)


class PaxosApplication(ProcessApplication[PaxosAggregate]):
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


class PaxosSystem(System):
    def __init__(self, num_participants: int = 3, **kwargs: Any):
        self.num_participants = num_participants
        self.quorum_size = (num_participants + 2) // 2
        classes = [
            type(
                "PaxosApplication{}".format(i),
                (PaxosApplication,),
                {"quorum_size": self.quorum_size},
            )
            for i in range(num_participants)
        ]
        assert num_participants > 1
        pipes = [[c[0], c[1], c[0]] for c in itertools.combinations(classes, 2)]
        super(PaxosSystem, self).__init__(pipes)
