import shlex
from concurrent.futures import Future
from queue import Queue
from time import time
from typing import Dict, List, Optional, Tuple, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.application import AggregateNotFound, ProcessEvent
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import IntegrityError, Transcoder
from eventsourcing.utils import get_topic, resolve_topic

from kvstore.domainmodel import (
    AppliesTo,
    KVAggregate,
    KVIndex,
    KVProposal,
    PaxosProposal,
)
from kvstore.exceptions import AggregateVersionMismatch, CommandRejected
from kvstore.transcodings import AppliesToAsList, KVProposalAsList
from paxos.application import PaxosApplication
from paxos.domainmodel import PaxosAggregate


class KVStore(PaxosApplication):
    snapshotting_intervals = {
        KVAggregate: 100,
        KVIndex: 100,
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

    def propose_command(
        self,
        cmd_text: str,
        assume_leader: bool = False,
        results_queue: "Optional[Queue[CommandFuture]]" = None,
    ) -> Future:
        kv_proposal = self.create_proposal(cmd_text)
        proposal_key = self.create_paxos_aggregate_id(kv_proposal.applies_to)
        paxos_proposal = PaxosProposal(proposal_key, kv_proposal)
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
        index = self.get_kv_index(command.key_name)
        if index is None:
            index_id = KVIndex.create_id(command.key_name)
            index_version = None
            aggregate_id = KVAggregate.create_id()
            aggregate_version = None
        else:
            index_id = index.id
            index_version = index.version
            aggregate_id = index.ref
            aggregate_version = self.get_kv_aggregate(aggregate_id).version
        applies_to = AppliesTo(
            aggregate_id=aggregate_id,
            aggregate_version=aggregate_version,
            index_id=index_id,
            index_version=index_version,
        )
        return KVProposal(
            cmd_text=cmd_text,
            applies_to=applies_to,
        )

    def get_or_create_kv_aggregate(
        self, command: "KVCommand", applies_to: AppliesTo
    ) -> Tuple[KVAggregate, Optional[KVIndex]]:
        aggregate = self.get_and_validate_kv_aggregate(applies_to)
        if aggregate:
            index = self.get_kv_index(command.key_name)
        else:
            aggregate, index = self.create_kv_aggregate(command, applies_to)
        return aggregate, index

    def get_and_validate_kv_aggregate(
        self, applies_to: AppliesTo
    ) -> Optional[KVAggregate]:
        aggregate = self.get_kv_aggregate(applies_to.aggregate_id)
        self.validate_kv_aggregate(aggregate, applies_to)
        return aggregate

    def get_kv_aggregate(self, aggregate_id: UUID) -> Optional[KVAggregate]:
        try:
            aggregate = cast(
                KVAggregate,
                self.repository.get(
                    aggregate_id,
                ),
            )
        except AggregateNotFound:
            aggregate = None
        return aggregate

    def validate_kv_aggregate(
        self, aggregate: Optional[KVAggregate], applies_to: AppliesTo
    ):
        if aggregate is not None:
            if applies_to.aggregate_version is None:
                raise AggregateVersionMismatch(
                    "Proposal applies to new aggregate "
                    f"but aggregate ID {applies_to.aggregate_id} "
                    f"version {aggregate.version} "
                    "was found in repository"
                )
            elif applies_to.aggregate_version != aggregate.version:
                raise AggregateVersionMismatch(
                    "Proposal applies to aggregate "
                    f"ID {applies_to.aggregate_id} "
                    f"version {applies_to.aggregate_version} "
                    "but aggregate in repository has "
                    f"version {aggregate.version}"
                )
        else:
            if applies_to.aggregate_version:
                raise AggregateVersionMismatch(
                    "Proposal applies to aggregate "
                    f"ID {applies_to.aggregate_id} "
                    f"version {applies_to.aggregate_version} "
                    "but aggregate was not found"
                )

    def create_kv_aggregate(
        self, command: "KVCommand", applies_to: AppliesTo
    ) -> [KVAggregate, KVIndex]:
        kv_aggregate = KVAggregate(applies_to.aggregate_id, command.key_name)
        kv_index = KVIndex(command.key_name, kv_aggregate.id)
        return kv_aggregate, kv_index

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

            process_event.save(paxos_aggregate)
            if resolution_msg:
                kv_aggregate, index1, index2 = self.execute_proposal(
                    paxos_aggregate.final_value
                )
                process_event.save(kv_aggregate)
                if index1:
                    process_event.save(index1)
                if index2:
                    process_event.save(index2)
                try:
                    self.futures[paxos_aggregate.id].set_result(time())
                except KeyError:
                    # Might not be the application which proposed the command.
                    pass

    def execute_proposal(
        self, kv_proposal: KVProposal
    ) -> Tuple[KVAggregate, Optional[KVIndex], Optional[KVIndex]]:
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

    def get_kv_index(self, key_name: str) -> Optional[KVIndex]:
        index_id = KVIndex.create_id(key_name)
        try:
            return cast(
                KVIndex,
                self.repository.get(
                    index_id,
                ),
            )
        except AggregateNotFound:
            return None


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

    def execute(
        self, app: KVStore, applies_to: AppliesTo
    ) -> Tuple[KVAggregate, Optional[KVIndex], Optional[KVIndex]]:
        pass


class CommandFuture(Future):
    def __init__(self, results_queue: "Optional[Queue[CommandFuture]]"):
        super(CommandFuture, self).__init__()
        self.started = time()
        if results_queue:
            self.add_done_callback(lambda future: results_queue.put(future))
