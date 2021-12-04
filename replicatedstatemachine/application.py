from abc import ABC, abstractmethod
from concurrent.futures import Future
from queue import Queue
from time import time
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
)
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.application import ProcessEvent
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import IntegrityError
from eventsourcing.utils import resolve_topic

from paxossystem.cache import Cache, LRUCache
from replicatedstatemachine.domainmodel import PaxosLogged
from replicatedstatemachine.eventsourcedlog import EventSourcedLog
from replicatedstatemachine.exceptions import CommandRejected
from paxossystem.application import PaxosApplication
from paxossystem.domainmodel import PaxosAggregate


class CommandFuture(Future):
    def __init__(self, cmd_text: str):
        super(CommandFuture, self).__init__()
        self.original_cmd_text = cmd_text
        self.started: float = time()
        self.finished: Optional[float] = None


class StateMachineReplica(PaxosApplication):
    COMMAND_CLASS = "COMMAND_CLASS"
    FUTURES_CACHE_MAXSIZE = "FUTURES_CACHE_MAXSIZE"
    pull_section_size = 100
    log_section_size = 100

    def __init__(self, env: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(env)
        command_class_topic = self.env.get(self.COMMAND_CLASS)
        self.command_class: Type[Command] = resolve_topic(command_class_topic)
        futures_cache_maxsize_envvar = self.env.get(self.FUTURES_CACHE_MAXSIZE, "1000")
        if futures_cache_maxsize_envvar:
            futures_cache_maxsize = int(futures_cache_maxsize_envvar)
            self.futures: Cache[UUID, CommandFuture] = LRUCache(maxsize=futures_cache_maxsize)
        else:
            self.futures = Cache()

        self.paxos_log: EventSourcedLog[PaxosLogged] = EventSourcedLog(
            self.events, uuid5(NAMESPACE_URL, "/paxoslog"), PaxosLogged
        )
        self.next_paxos_round: Optional[int] = None

    def propose_command(self, cmd_text: str) -> CommandFuture:
        future = CommandFuture(cmd_text=cmd_text)
        if self.num_participants > 1:
            paxos_logged = self.paxos_log.trigger_event(self.next_paxos_round)
            proposal_key = self.create_paxos_aggregate_id_from_round(
                paxos_logged.originator_version
            )
            self.futures.put(proposal_key, future)
            paxos_aggregate = self.start_paxos(proposal_key, cmd_text)
            try:
                self.save(paxos_aggregate, paxos_logged)
            except IntegrityError as e:
                self.next_paxos_round = None
                raise CommandRejected from e
            else:
                self.next_paxos_round = paxos_logged.originator_version + 1
        else:
            result = self.execute_proposal(cmd_text)
            future.finished = time()
            future.set_result(result)
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
            # Todo: Think about passing the round number with the proposed value.
            if len(paxos_aggregate.pending_events) == paxos_aggregate.version:
                paxos_logged = self.paxos_log.trigger_event(self.next_paxos_round)
                paxos_id = self.create_paxos_aggregate_id_from_round(
                    paxos_logged.originator_version
                )
                if paxos_id != paxos_aggregate.id:
                    raise Exception("Out of order paxoses :-(")
                process_event.save(paxos_logged)

            if resolution_msg:
                try:
                    future: Optional[CommandFuture] = self.futures.get(paxos_aggregate.id)
                except KeyError:
                    future = None
                try:
                    result = self.execute_proposal(paxos_aggregate.final_value)
                except Exception as e:
                    if future:
                        future.set_exception(exception=e)
                else:
                    if future:
                        future.finished = time()
                        if future.original_cmd_text != paxos_aggregate.final_value:
                            future.set_exception(
                                CommandRejected(
                                    "Executed other command '{}' not original '{}'".format(
                                        paxos_aggregate.final_value,
                                        future.original_cmd_text,
                                    )
                                )
                            )
                        else:
                            future.set_result(result)

    def execute_proposal(self, cmd_text: str) -> Any:
        cmd = self.command_class.parse(cmd_text)
        aggregates, result = cmd.execute(self)
        self.save(*aggregates)
        return result

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
    def execute(self, app: StateMachineReplica) -> Tuple[Tuple[Aggregate, ...], Any]:
        pass

    @abstractmethod
    def do_query(self, app: StateMachineReplica) -> Any:
        pass
