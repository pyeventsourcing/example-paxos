import itertools
from typing import Any, Optional, Sequence, Union
from uuid import UUID

from eventsourcing.application.process import (
    ProcessApplication,
    WrappedRepository,
)
from eventsourcing.system.definition import System
from paxos.composable import (
    PaxosMessage,
)
from eventsourcing.domain.model.decorators import retry
from eventsourcing.exceptions import (
    OperationalError,
    RecordConflictError,
    RepositoryKeyError,
)

from paxos.domainmodel import PaxosAggregate


class PaxosApplication(ProcessApplication[PaxosAggregate, PaxosAggregate.Event]):
    persist_event_type = PaxosAggregate.Event
    quorum_size: int = 0
    notification_log_section_size = 5
    use_cache = True
    set_notification_ids = True

    @retry((RecordConflictError, OperationalError), max_attempts=10, wait=0)
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
        return self.repository[key].final_value

    def policy(
        self,
        repository: WrappedRepository[PaxosAggregate, PaxosAggregate.Event],
        event: PaxosAggregate.Event,
    ) -> Optional[Union[PaxosAggregate, Sequence[PaxosAggregate]]]:
        """
        Processes paxos "message announced" events of other applications
        by starting or continuing a paxos aggregate in this application.
        """
        if isinstance(event, PaxosAggregate.MessageAnnounced):
            # Get or create aggregate.
            try:
                paxos = repository[event.originator_id]
            except RepositoryKeyError:
                paxos = PaxosAggregate.start(
                    originator_id=event.originator_id,
                    quorum_size=self.quorum_size,
                    network_uid=self.name,
                )
                # Needs to go in the cache now, otherwise we get
                # "Duplicate" errors (for some unknown reason).
                if self.repository.use_cache:
                    self.repository.put_entity_in_cache(paxos.id, paxos)
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
            msg = event.msg
            assert isinstance(msg, PaxosMessage)
            if paxos.final_value is None:
                paxos.receive_message(msg)

            return paxos


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
        pipelines = [[c[0], c[1], c[0]] for c in itertools.combinations(classes, 2)]
        super(PaxosSystem, self).__init__(*pipelines, **kwargs)
