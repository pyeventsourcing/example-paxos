import itertools
import shlex
from concurrent.futures import Future
from dataclasses import dataclass
from decimal import Decimal
from queue import Queue
from threading import RLock
from time import time
from typing import Any, Dict, List, Optional, cast
from uuid import UUID

from eventsourcing.application import AggregateNotFound, ProcessEvent, Repository
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import IntegrityError, Transcoder, Transcoding
from eventsourcing.system import System
from eventsourcing.utils import get_topic

from kvstore.domainmodel import (
    HashAggregate,
    KVAggregate,
    create_kv_aggregate_id,
    create_paxos_aggregate_id,
)
from paxos.application import PaxosApplication
from paxos.domainmodel import PaxosAggregate


@dataclass
class AppliesTo:
    aggregate_id: UUID
    aggregate_version: Optional[int]


@dataclass
class KVProposal:
    cmd: List[str]
    applies_to: AppliesTo


@dataclass
class PaxosProposal:
    key: UUID
    value: KVProposal


def propose_command(app: "KVStore", cmd_text: str) -> PaxosProposal:
    return Command.construct(split(cmd_text)).create_paxos_proposal(app)


def execute_proposal(app: "KVStore", kv_proposal: KVProposal):
    cmd = Command.construct(kv_proposal.cmd)
    return cmd.execute(app, kv_proposal.applies_to)


def split(text: str) -> List[str]:
    return shlex.split(text)


class Command:
    @classmethod
    def construct(cls, cmd: List[str]) -> "Command":
        command_class = globals()[cmd[0].upper() + "Command"]
        return command_class(cmd)

    def __init__(self, cmd: List[str]):
        self.cmd = cmd

    def create_paxos_proposal(self, app: "KVStore") -> PaxosProposal:
        pass

    def do_query(self, app: "KVStore"):
        pass

    def execute(self, app: "KVStore", applies_to: AppliesTo) -> KVAggregate:
        pass


class CommandRejected(Exception):
    pass


class OperationOnKey(Command):
    @property
    def key_name(self) -> str:
        return self.cmd[1]

    def create_paxos_proposal(self, app: "KVStore"):
        aggregate_id = create_kv_aggregate_id(self.key_name)
        aggregate_version = self.get_current_version(app, aggregate_id)
        proposal_key = create_paxos_aggregate_id(aggregate_id, aggregate_version)
        proposal_value = KVProposal(
            cmd=self.cmd,
            applies_to=AppliesTo(
                aggregate_id=aggregate_id,
                aggregate_version=aggregate_version,
            ),
        )
        return PaxosProposal(proposal_key, proposal_value)

    @staticmethod
    def get_current_version(app: "KVStore", aggregate_id: UUID) -> Optional[UUID]:
        try:
            last_event = next(
                app.repository.event_store.get(aggregate_id, desc=True, limit=1)
            )
        except StopIteration:
            aggregate_version = None
        else:
            aggregate_version = last_event.originator_version
        return aggregate_version

    @staticmethod
    def get_aggregate(app: "KVStore", applies_to: AppliesTo) -> Optional[KVAggregate]:
        try:
            aggregate = cast(KVAggregate, app.repository.get(applies_to.aggregate_id))
        except AggregateNotFound:
            if applies_to.aggregate_version is not None:
                raise AggregateVersionMismatch(
                    "Proposal applies to aggregate "
                    f"ID {applies_to.aggregate_id} "
                    f"version {applies_to.aggregate_version} "
                    "but aggregate not found in repository"
                )
            else:
                return None
        else:
            if applies_to.aggregate_version != aggregate.version:
                raise AggregateVersionMismatch(
                    "Proposal applies to aggregate "
                    f"ID {applies_to.aggregate_id} "
                    f"version {applies_to.aggregate_version} "
                    "but aggregate in repository has "
                    f"version {aggregate.version}"
                )
            else:
                return aggregate


class AggregateVersionMismatch(Exception):
    pass


class HashCommand(OperationOnKey):
    @staticmethod
    def get_hash_aggregate(
        app: "KVStore", applies_to: AppliesTo
    ) -> Optional[HashAggregate]:
        return cast(
            Optional[HashAggregate], OperationOnKey.get_aggregate(app, applies_to)
        )

    def create_hash_aggregate(self, app: "KVStore") -> HashAggregate:
        aggregate_id = create_kv_aggregate_id(self.key_name)
        aggregate = HashAggregate.create(aggregate_id, self.key_name)
        app.repository.cache.put(aggregate_id, aggregate)
        return aggregate

    def get_or_create_hash_aggregate(
        self, app: "KVStore", applies_to: AppliesTo
    ) -> HashAggregate:
        return self.get_hash_aggregate(app, applies_to) or self.create_hash_aggregate(
            app
        )


class HSETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(self, app: "KVStore", applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        aggregate.set_field_value(self.field_name, self.field_value)
        return aggregate


class HSETNXCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(self, app: "KVStore", applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        if self.field_name not in aggregate.hash:
            aggregate.set_field_value(self.field_name, self.field_value)
        return aggregate


class HINCRBYCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def incr_by(self) -> Decimal:
        return Decimal(self.cmd[3])

    def execute(self, app: "KVStore", applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        aggregate.set_field_value(
            self.field_name,
            str(
                self.incr_by + Decimal(aggregate.get_field_value(self.field_name) or 0)
            ),
        )
        return aggregate


class HDELCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def execute(self, app: "KVStore", applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_hash_aggregate(app, applies_to)
        if aggregate is not None:
            try:
                aggregate.del_field_value(self.field_name)
            except KeyError:
                pass
        return aggregate


class HGETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def do_query(self, app: "KVStore"):
        aggregate_id = create_kv_aggregate_id(self.key_name)
        try:
            aggregate = app.repository.get(aggregate_id)
        except AggregateNotFound:
            return None
        else:
            assert isinstance(aggregate, HashAggregate), aggregate
            return aggregate.get_field_value(self.field_name)


class CachedRepository(Repository):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = Cache(maxsize=500)

    def get(self, aggregate_id) -> TAggregate:
        try:
            return self.cache.get(aggregate_id)
        except KeyError:
            aggregate = super().get(aggregate_id)
            self.cache.put(aggregate_id, aggregate)
            return aggregate


class Cache:
    sentinel = object()  # unique object used to signal cache misses
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3  # names for the link fields

    def __init__(self, maxsize):
        # Constants shared by all lru cache instances:

        self.maxsize = maxsize
        self.cache = {}
        self.full = False
        self.lock = RLock()  # because linkedlist updates aren't threadsafe
        self.root = []  # root of the circular doubly linked list
        self.root[:] = [
            self.root,
            self.root,
            None,
            None,
        ]  # initialize by pointing to self

    def get(self, key):
        # Size limited caching that tracks accesses by recency
        with self.lock:
            link = self.cache.get(key)
            if link is not None:
                # Move the link to the front of the circular queue
                link_prev, link_next, _key, result = link
                link_prev[Cache.NEXT] = link_next
                link_next[Cache.PREV] = link_prev
                last = self.root[Cache.PREV]
                last[Cache.NEXT] = self.root[Cache.PREV] = link
                link[Cache.PREV] = last
                link[Cache.NEXT] = self.root
                return result
            else:
                raise KeyError

    def put(self, key, value):
        with self.lock:
            if key in self.cache:
                # Getting here means that this same key was added to the
                # cache while the lock was released.  Since the link
                # update is already done, we need only return the
                # computed result and update the count of misses.
                pass
            elif self.full:
                # Use the old root to store the new key and result.
                oldroot = self.root
                oldroot[Cache.KEY] = key
                oldroot[Cache.RESULT] = value
                # Empty the oldest link and make it the new root.
                # Keep a reference to the old key and old result to
                # prevent their ref counts from going to zero during the
                # update. That will prevent potentially arbitrary object
                # clean-up code (i.e. __del__) from running while we're
                # still adjusting the links.
                self.root = oldroot[Cache.NEXT]
                oldkey = self.root[Cache.KEY]
                _ = self.root[Cache.RESULT]
                self.root[Cache.KEY] = self.root[Cache.RESULT] = None
                # Now update the cache dictionary.
                del self.cache[oldkey]
                # Save the potentially reentrant cache[key] assignment
                # for last, after the root and links have been put in
                # a consistent state.
                self.cache[key] = oldroot
            else:
                # Put result in a new link at the front of the queue.
                last = self.root[Cache.PREV]
                link = [last, self.root, key, value]
                last[Cache.NEXT] = self.root[Cache.PREV] = self.cache[key] = link
                # Use the cache_len bound method instead of the len() function
                # which could potentially be wrapped in an lru_cache itself.
                self.full = self.cache.__len__() >= self.maxsize


class KVProposalAsList(Transcoding):
    name = "k_v_proposal"
    type = KVProposal

    def encode(self, obj: KVProposal) -> List:
        return [obj.cmd, obj.applies_to]

    def decode(self, data: List) -> KVProposal:
        return KVProposal(*data)


class AppliesToAsList(Transcoding):
    name = "applies_to"
    type = AppliesTo

    def encode(self, obj: AppliesTo) -> List:
        return [obj.aggregate_id, obj.aggregate_version]

    def decode(self, data: List) -> AppliesTo:
        return AppliesTo(*data)


class CommandFuture(Future):
    def __init__(self, results_queue: "Optional[Queue[CommandFuture]]"):
        super(CommandFuture, self).__init__()
        self.started = time()
        if results_queue:
            self.add_done_callback(lambda future: results_queue.put(future))


class KVStore(PaxosApplication):
    snapshotting_intervals = {
        # PaxosAggregate: 50,
        # HashAggregate: 100,
    }
    follow_topics = [
        get_topic(PaxosAggregate.MessageAnnounced),
    ]
    pull_section_size = 100
    log_section_size = 100

    def __init__(self):
        super(KVStore, self).__init__()
        self.futures: Dict[UUID, Future] = {}

    def register_transcodings(self, transcoder: Transcoder) -> None:
        super().register_transcodings(transcoder)
        transcoder.register(KVProposalAsList())
        transcoder.register(AppliesToAsList())

    def construct_repository(self) -> Repository[TAggregate]:
        return CachedRepository(
            event_store=self.events,
            snapshot_store=self.snapshots,
        )

    def propose_command(
        self,
        cmd_text: str,
        assume_leader: bool = False,
        results_queue: "Optional[Queue[CommandFuture]]" = None,
    ) -> Future:
        proposal = propose_command(self, cmd_text)
        future = CommandFuture(results_queue=results_queue)
        self.futures[proposal.key] = future
        try:
            self.propose_value(proposal.key, proposal.value, assume_leader)
        except IntegrityError as e:
            raise CommandRejected from e
        else:
            return future

    def execute_query(self, cmd_text: str):
        cmd = Command.construct(split(cmd_text))
        return cmd.do_query(self)

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
            paxos_aggregate, resolution_msg = self.process_message_announced(
                domain_event
            )
            self.repository.cache.put(paxos_aggregate.id, paxos_aggregate)

            process_event.save(paxos_aggregate)
            if resolution_msg:
                proposal: KVProposal = paxos_aggregate.final_value
                cmd = Command.construct(proposal.cmd)
                kv_aggregate = cmd.execute(self, proposal.applies_to)
                self.repository.cache.put(kv_aggregate.id, kv_aggregate)
                process_event.save(kv_aggregate)
                try:
                    self.futures[paxos_aggregate.id].set_result(time())
                except KeyError:
                    # Might not be the application which proposed the command.
                    pass

    def notify(self, new_events: List[AggregateEvent[Aggregate]]) -> None:
        """
        Extends the application :func:`~eventsourcing.application.Application.notify`
        method by calling :func:`prompt_followers` whenever new events have just
        been saved.
        """
        for new_event in new_events:
            if isinstance(new_event, PaxosAggregate.MessageAnnounced):
                self.prompt_followers()
                return


class KVSystem(System):
    def __init__(self, num_participants: int = 3, **kwargs: Any):
        self.num_participants = num_participants
        self.quorum_size = (num_participants + 2) // 2
        classes = [
            type(
                "KVStore{}".format(i),
                (KVStore,),
                {"quorum_size": self.quorum_size},
            )
            for i in range(num_participants)
        ]
        assert num_participants > 1
        pipes = [[c[0], c[1], c[0]] for c in itertools.combinations(classes, 2)]
        super(KVSystem, self).__init__(pipes)
