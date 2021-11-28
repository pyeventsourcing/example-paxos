import shlex
from concurrent.futures import Future
from queue import Queue
from time import time
from typing import Dict, List, Optional, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.application import AggregateNotFound, ProcessEvent, Repository
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import IntegrityError, Transcoder
from eventsourcing.utils import get_topic, resolve_topic

from kvstore.cache import CachedRepository
from kvstore.domainmodel import (
    AppliesTo,
    KVAggregate,
    KVProposal,
    PaxosProposal,
)
from kvstore.exceptions import AggregateVersionMismatch, CommandRejected
from kvstore.transcodings import AppliesToAsList, KVProposalAsList
from paxos.application import PaxosApplication
from paxos.domainmodel import PaxosAggregate


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
        kv_proposal = self.create_proposal(cmd_text)
        proposal_key = self.create_paxos_aggregate_id(kv_proposal.applies_to)
        paxos_proposal = PaxosProposal(
            proposal_key,
            kv_proposal
        )
        future = CommandFuture(results_queue=results_queue)
        self.futures[paxos_proposal.key] = future
        try:
            self.propose_value(paxos_proposal.key, paxos_proposal.value, assume_leader)
        except IntegrityError as e:
            raise CommandRejected from e
        else:
            return future

    @staticmethod
    def create_paxos_aggregate_id(applies_to: AppliesTo) -> UUID:
        return uuid5(
            NAMESPACE_URL,
            f"/proposals/{applies_to.aggregate_id}/{applies_to.aggregate_version}",
        )

    def create_proposal(self, cmd_text: str) -> KVProposal:
        command = KVCommand.construct(cmd_text)
        applies_to_id = KVAggregate.create_id(command.key_name)
        applies_to_version = self.get_applies_to_version(applies_to_id)
        applies_to = AppliesTo(aggregate_id=applies_to_id, aggregate_version=applies_to_version)
        return KVProposal(
            cmd_text=cmd_text,
            applies_to=applies_to,
        )

    def get_applies_to_version(self, aggregate_id: UUID) -> Optional[int]:
        try:
            last_event = next(
                self.repository.event_store.get(aggregate_id, desc=True, limit=1)
            )
        except StopIteration:
            aggregate_version = 1
        else:
            aggregate_version = last_event.originator_version
        return aggregate_version

    def get_or_create_kv_aggregate(
        self, command: "KVCommand", aggregate_id: UUID, expect_version: Optional[int] = None
    ) -> KVAggregate:
        return self.get_kv_aggregate(aggregate_id, expect_version) or self.create_kv_aggregate(command)

    def get_kv_aggregate(self, aggregate_id: UUID, expect_version: Optional[int] = None) -> Optional[KVAggregate]:
        try:
            aggregate = cast(KVAggregate, self.repository.get(aggregate_id))
        except AggregateNotFound:
            aggregate = None

        if expect_version is not None:
            if expect_version == 1:
                if aggregate is not None:
                    raise AggregateVersionMismatch(
                        "Proposal applies to new aggregate "
                        f"but aggregate ID {aggregate_id} "
                        f"version {aggregate.version} "
                        "was found in repository"
                    )
            elif expect_version > 1:
                if aggregate is None:
                    raise AggregateVersionMismatch(
                        "Proposal applies to aggregate "
                        f"ID {aggregate_id} "
                        f"version {expect_version} "
                        "but aggregate not found in repository"
                    )
                elif expect_version != aggregate.version:
                    raise AggregateVersionMismatch(
                        "Proposal applies to aggregate "
                        f"ID {aggregate_id} "
                        f"version {expect_version} "
                        "but aggregate in repository has "
                        f"version {aggregate.version}"
                    )
        return aggregate

    def create_kv_aggregate(self, command: "KVCommand") -> KVAggregate:
        aggregate = KVAggregate(command.key_name)
        self.repository.cache.put(aggregate.id, aggregate)
        return aggregate

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
                kv_aggregate = self.execute_proposal(paxos_aggregate.final_value)
                self.repository.cache.put(kv_aggregate.id, kv_aggregate)
                process_event.save(kv_aggregate)
                try:
                    self.futures[paxos_aggregate.id].set_result(time())
                except KeyError:
                    # Might not be the application which proposed the command.
                    pass

    def execute_proposal(self, kv_proposal: KVProposal) -> KVAggregate:
        cmd = KVCommand.construct(kv_proposal.cmd_text)
        return cmd.execute(self, kv_proposal.applies_to)

    def execute_query(self, cmd_text: str):
        cmd = KVCommand.construct(cmd_text)
        return cmd.do_query(self)

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


def split(text: str) -> List[str]:
    return shlex.split(text)


class KVCommand:
    @classmethod
    def construct(cls, cmd_text: str) -> "KVCommand":
        cmd = split(cmd_text)
        commands = resolve_topic("kvstore.commands")
        command_class = commands.__dict__[cmd[0].upper() + "Command"]
        return command_class(cmd)

    def __init__(self, cmd: List[str]):
        self.cmd = cmd

    @property
    def key_name(self) -> str:
        return self.cmd[1]

    def do_query(self, app: KVStore):
        pass

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        pass


class CommandFuture(Future):
    def __init__(self, results_queue: "Optional[Queue[CommandFuture]]"):
        super(CommandFuture, self).__init__()
        self.started = time()
        if results_queue:
            self.add_done_callback(lambda future: results_queue.put(future))
