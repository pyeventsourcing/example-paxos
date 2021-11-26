import shlex
from concurrent.futures import Future
from queue import Queue
from time import time
from typing import Dict, List, Optional
from uuid import UUID

from eventsourcing.application import ProcessEvent, Repository
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import IntegrityError, Transcoder
from eventsourcing.utils import get_topic

from kvstore.cache import CachedRepository
from kvstore.domainmodel import (
    AppliesTo,
    KVAggregate,
    KVProposal,
    PaxosProposal,
)
from kvstore.exceptions import CommandRejected
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
        from kvstore import commands

        command_class = commands.__dict__[cmd[0].upper() + "Command"]
        return command_class(cmd)

    def __init__(self, cmd: List[str]):
        self.cmd = cmd

    def create_paxos_proposal(self, app: "KVStore") -> PaxosProposal:
        pass

    def do_query(self, app: "KVStore"):
        pass

    def execute(self, app: "KVStore", applies_to: AppliesTo) -> KVAggregate:
        pass


class CommandFuture(Future):
    def __init__(self, results_queue: "Optional[Queue[CommandFuture]]"):
        super(CommandFuture, self).__init__()
        self.started = time()
        if results_queue:
            self.add_done_callback(lambda future: results_queue.put(future))
