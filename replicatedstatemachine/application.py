from abc import ABC, abstractmethod
from concurrent.futures import Future, InvalidStateError
from time import time
from typing import (
    Any,
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
from eventsourcing.utils import resolve_topic, retry

from paxossystem.cache import Cache, LRUCache
from replicatedstatemachine.domainmodel import PaxosLogged
from replicatedstatemachine.eventsourcedlog import EventSourcedLog
from replicatedstatemachine.exceptions import CommandErrored, CommandFutureEvicted, CommandRejected
from paxossystem.application import PaxosApplication
from paxossystem.domainmodel import PaxosAggregate

_print = print

debug = False


def print(*args):
    if debug:
        _print(*args)


class CommandFuture(Future[Any]):
    def __init__(self, cmd_text: str):
        super(CommandFuture, self).__init__()
        self.original_cmd_text = cmd_text
        self.started: float = time()
        self.finished: Optional[float] = None

    def set_exception(self, exception: Optional[BaseException]) -> None:
        self.finished = time()
        super().set_exception(exception)

    def set_result(self, result: Any) -> None:
        self.finished = time()
        super().set_result(result)


class StateMachineReplica(PaxosApplication):
    COMMAND_CLASS = "COMMAND_CLASS"
    FUTURES_CACHE_MAXSIZE = "FUTURES_CACHE_MAXSIZE"
    pull_section_size = 100
    log_section_size = 100
    futures_cache_maxsize = 1000
    log_read_commands = True

    def __init__(self, env: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(env)
        command_class_topic = self.env.get(self.COMMAND_CLASS)
        self.command_class: Type[Command] = resolve_topic(command_class_topic)
        futures_cache_maxsize_envvar = self.env.get(self.FUTURES_CACHE_MAXSIZE)
        if futures_cache_maxsize_envvar:
            futures_cache_maxsize = int(futures_cache_maxsize_envvar)
        else:
            futures_cache_maxsize = self.futures_cache_maxsize
        if futures_cache_maxsize > 0:
            self.futures: Cache[UUID, CommandFuture] = LRUCache(maxsize=futures_cache_maxsize)
        else:
            self.futures = Cache()

        self.paxos_log: EventSourcedLog[PaxosLogged] = EventSourcedLog(
            self.events, uuid5(NAMESPACE_URL, "/paxoslog"), PaxosLogged
        )

    def propose_command(self, cmd_text: str) -> CommandFuture:
        cmd = self.command_class.parse(cmd_text)
        future = CommandFuture(cmd_text=cmd_text)
        if (self.log_read_commands or cmd.mutates_state) and self.num_participants > 1:

            # Start a paxos round.
            paxos_logged = self.paxos_log.trigger_event()
            proposal_key = self.create_paxos_aggregate_id_from_round(
                round=paxos_logged.originator_version
            )
            paxos_aggregate = self.start_paxos(proposal_key, cmd_text, self.assume_leader)

            # Put the future in cache, so can set result after reaching consensus.
            evicted_future = self.futures.put(proposal_key, future)
            if evicted_future is not None:
                # Finish an evicted future, if not already finished.
                try:
                    evicted_future.set_exception(CommandFutureEvicted("Futures cache full"))
                except InvalidStateError:
                    pass

            # Save the Paxos aggregate.
            try:
                self.save(paxos_aggregate, paxos_logged)
            except IntegrityError as e:
                msg = f"{self.name}: Rejecting command for round {paxos_logged.originator_version}"
                if list(self.events.get(paxos_aggregate.id)):
                    msg += f": Already have paxos aggregate: {paxos_aggregate.id}"
                error = CommandRejected(msg)

                # Evict the future from the cache.
                try:
                    self.futures.get(paxos_aggregate.id, evict=True)
                except KeyError:
                    pass

                # Set error on future.
                try:
                    future.set_exception(error)
                except InvalidStateError:
                    pass
                # raise CommandRejected from e
        else:
            # Just execute the command immediately.
            try:
                cmd = self.command_class.parse(cmd_text)
                aggregates, result = cmd.execute(self)
                self.save(*aggregates)
            except Exception as e:
                future.set_exception(CommandErrored(e))
            else:
                future.set_result(result)

        return future

    @staticmethod
    def create_paxos_aggregate_id_from_round(round: int) -> UUID:
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
            print(self.name, domain_event.originator_id, "processing", domain_event.msg)
            try:
                paxos_aggregate, resolution_msg = self.process_message_announced(domain_event)
            except Exception as e:
                error_msg = f"{self.name} {domain_event.originator_id} errored processing {domain_event.msg}"
                print(error_msg, e)
                raise Exception(error_msg) from e

            # Log new paxos aggregate.
            if len(paxos_aggregate.pending_events) == paxos_aggregate.version:
                paxos_logged = self.paxos_log.trigger_event()
                paxos_id = self.create_paxos_aggregate_id_from_round(
                    paxos_logged.originator_version
                )
                # Check the paxos log isn't out of order.
                if paxos_id != paxos_aggregate.id:
                    # Out of order commands.
                    error = CommandRejected("Out of order paxoses")
                    _print(error)
                    try:
                        future: Optional[CommandFuture] = self.futures.get(paxos_aggregate.id, evict=True)
                    except KeyError:
                        print(self.name, f"future not found for {paxos_aggregate.id}")
                        pass
                    else:
                        try:
                            future.set_exception(error)
                        except InvalidStateError:
                            pass
                    paxos_aggregate = None
                    paxos_logged = None
            else:
                paxos_logged = None

            if resolution_msg:
                try:
                    future: Optional[CommandFuture] = self.futures.get(paxos_aggregate.id, evict=True)
                except KeyError:
                    print(self.name, f"future not found for {paxos_aggregate.id}")
                    future = None
                try:
                    print(self.name, "executing", paxos_aggregate.final_value)
                    cmd = self.command_class.parse(paxos_aggregate.final_value)
                    if cmd.mutates_state or future:
                        aggregates, result = cmd.execute(self)
                    else:
                        _print("Skipped executing read command because has no future")
                        aggregates, result = (), None
                except Exception as e:
                    aggregates = ()
                    if future:
                        try:
                            future.set_exception(exception=CommandErrored(e))
                        except InvalidStateError:
                            pass
                else:
                    if future:
                        if future.original_cmd_text != paxos_aggregate.final_value:
                            try:
                                future.set_exception(
                                    CommandRejected(
                                        "Executed other command '{}'".format(
                                            paxos_aggregate.final_value,
                                        )
                                    )
                                )
                            except InvalidStateError:
                                pass
                        else:
                            try:
                                future.set_result(result)
                            except InvalidStateError:
                                pass
            else:
                aggregates = ()
            process_event.save(paxos_aggregate)
            process_event.save(paxos_logged)
            process_event.save(*aggregates)

    def record(self, process_event: ProcessEvent) -> Optional[int]:
        new_events = list(process_event.events)
        try:
            returning = super().record(process_event)
        except Exception:
            print(self.name, f"errored saving {len(process_event.events)} events")

            for new_event in new_events:
                print(
                    self.name, new_event.originator_id, "errored saving event",
                    new_event.originator_id, new_event.originator_version,
                    process_event.tracking and process_event.tracking.notification_id or None,
                    process_event.tracking and process_event.tracking.application_name or None
                )
                if list(self.recorder.select_events(
                    new_event.originator_id,
                        lte=new_event.originator_version,
                        gt=new_event.originator_version - 1,
                        limit=1
                )):
                    print("Already have recorded event for", new_event.originator_id, new_event.originator_version)
                else:
                    print("Don't have recorded event for", new_event.originator_id, new_event.originator_version)

            if process_event.tracking:
                max_tracking_id = self.recorder.max_tracking_id(process_event.tracking.application_name)
                if max_tracking_id >= process_event.tracking.notification_id:
                    print("Recorded tracking ID greater than process event")

            raise
        else:
            for new_event in new_events:
                print(
                    self.name, new_event.originator_id, "saved event",
                    new_event.originator_id, new_event.originator_version,
                    process_event.tracking and process_event.tracking.notification_id or None,
                    process_event.tracking and process_event.tracking.application_name or None
                )
        return returning


    def notify(self, new_events: List[AggregateEvent[Aggregate]]) -> None:
        """
        Extends the application :func:`~eventsourcing.application.Application.notify`
        method by calling :func:`prompt_followers` whenever new events have just
        been saved.
        """
        for new_event in new_events:
            if isinstance(new_event, PaxosAggregate.MessageAnnounced):
                print(self.name, new_event.originator_id, "saved", new_event.msg, new_event.originator_id, new_event.originator_version)
        super().notify(new_events)


class Command(ABC):
    mutates_state = True

    @classmethod
    @abstractmethod
    def parse(cls, cmd_text: str) -> "Command":
        pass

    @abstractmethod
    def execute(self, app: StateMachineReplica) -> Tuple[Tuple[Aggregate, ...], Any]:
        pass
