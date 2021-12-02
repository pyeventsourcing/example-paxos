from abc import ABC, abstractmethod
from concurrent.futures import Future
from queue import Queue
from time import time
from typing import (
    Any,
    Dict,
    Generic,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    cast,
)
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.application import ProcessEvent
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate, TDomainEvent
from eventsourcing.persistence import EventStore, IntegrityError
from eventsourcing.utils import resolve_topic

from replicatedstatemachine.exceptions import CommandRejected
from paxossystem.application import PaxosApplication
from paxossystem.domainmodel import PaxosAggregate


class StateMachineReplica(PaxosApplication):
    COMMAND_CLASS = "COMMAND_CLASS"
    pull_section_size = 100
    log_section_size = 100

    def __init__(self, env: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(env)
        self.futures: Dict[UUID, CommandFuture] = {}
        self.paxos_log: Log[PaxosLogged] = Log(
            self.events, uuid5(NAMESPACE_URL, "/paxoslog"), PaxosLogged
        )
        self.next_paxos_round: Optional[int] = None
        command_class_topic = self.env.get(self.COMMAND_CLASS)
        self.command_class: Type[Command] = resolve_topic(command_class_topic)

    def propose_command(
        self,
        cmd_text: str,
        assume_leader: bool = False,
        results_queue: "Optional[Queue[CommandFuture]]" = None,
    ) -> Future:
        paxos_logged = self.paxos_log.trigger_event(self.next_paxos_round)
        proposal_key = self.create_paxos_aggregate_id_from_round(
            paxos_logged.originator_version
        )
        future = CommandFuture(cmd_text=cmd_text, results_queue=results_queue)
        self.futures[proposal_key] = future
        paxos_aggregate = self.start_paxos(proposal_key, cmd_text, assume_leader)

        try:
            self.save(paxos_aggregate, paxos_logged)
        except IntegrityError as e:
            self.next_paxos_round = None
            raise CommandRejected from e
        else:
            self.next_paxos_round = paxos_logged.originator_version + 1
            return future

    def create_paxos_aggregate_id_from_round(self, round: int) -> UUID:
        return uuid5(
            NAMESPACE_URL,
            f"/proposals/{round}",
        )

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
                aggregates = self.execute_proposal(paxos_aggregate.final_value)
                process_event.save(*aggregates)
                try:
                    future = self.futures[paxos_aggregate.id]
                except KeyError:
                    # Might not be the application which proposed the command.
                    pass
                else:

                    future.finished = time()
                    # Todo: Check original_cmd_text equals final value, otherwise raise an error.
                    # Todo: Capture that, and any other actual command execution errors, and call set_exception().
                    # Todo: Get actual command execution results and call set_result().
                    future.set_result("DONE")

    def execute_proposal(self, cmd_text: str) -> Tuple[Aggregate, ...]:
        cmd = self.command_class.parse(cmd_text)
        return cmd.execute(self)

    def execute_query(self, cmd_text: str):
        cmd = self.command_class.parse(cmd_text)
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


class Command(ABC):
    @classmethod
    @abstractmethod
    def parse(cls, cmd_text: str) -> "Command":
        pass

    @abstractmethod
    def execute(self, app: StateMachineReplica) -> Tuple[Aggregate, ...]:
        pass

    @abstractmethod
    def do_query(self, app: StateMachineReplica) -> Any:
        pass


class CommandFuture(Future):
    def __init__(self, cmd_text: str, results_queue: "Optional[Queue[CommandFuture]]"):
        super(CommandFuture, self).__init__()
        self.original_cmd_text = cmd_text
        self.started: float = time()
        self.finished: Optional[float] = None
        if results_queue:
            self.add_done_callback(lambda future: results_queue.put(future))


class Log(Generic[TDomainEvent]):
    def __init__(
        self,
        events: EventStore[AggregateEvent[Aggregate]],
        originator_id: UUID,
        logged_cls: Type[TDomainEvent],
    ):
        self.events = events
        self.originator_id = originator_id
        self.logged_cls = logged_cls

    def trigger_event(self, next_originator_version: Optional[int]) -> TDomainEvent:
        if next_originator_version is None:
            last_logged = self._get_last_logged()
            if last_logged:
                next_originator_version = last_logged.originator_version + 1
            else:
                next_originator_version = Aggregate.INITIAL_VERSION
        return self.logged_cls(  # type: ignore
            originator_id=self.originator_id,
            originator_version=next_originator_version,
            timestamp=self.logged_cls.create_timestamp(),
        )

    def get(self, limit: int = 10, offset: int = 0) -> Iterator[TDomainEvent]:
        # Calculate lte.
        lte = None
        if offset > 0:
            last = self._get_last_logged()
            if last:
                lte = last.originator_version - offset

        # Get logged events.
        return cast(
            Iterator[TDomainEvent],
            self.events.get(
                originator_id=self.originator_id,
                lte=lte,
                desc=True,
                limit=limit,
            ),
        )

    def _get_last_logged(
        self,
    ) -> Optional[TDomainEvent]:
        events = self.events.get(originator_id=self.originator_id, desc=True, limit=1)
        try:
            return cast(TDomainEvent, next(events))
        except StopIteration:
            return None


class PaxosLogged(AggregateEvent[Aggregate]):
    pass