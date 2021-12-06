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
from replicatedstatemachine.exceptions import (
    CommandExecutionError,
    PaxosProtocolError,
    CommandFutureEvicted,
    CommandRejected,
)
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
        # Decide base command class.
        self.command_class: Type[Command] = resolve_topic(
            self.env.get(self.COMMAND_CLASS)
        )

        # Construct cache for command futures.
        futures_cache_maxsize_envvar = self.env.get(self.FUTURES_CACHE_MAXSIZE)
        if futures_cache_maxsize_envvar:
            futures_cache_maxsize = int(futures_cache_maxsize_envvar)
        else:
            futures_cache_maxsize = self.futures_cache_maxsize
        if futures_cache_maxsize > 0:
            self.futures: Cache[UUID, CommandFuture] = LRUCache(
                maxsize=futures_cache_maxsize
            )
        else:
            self.futures = Cache()

        # Construct log of paxos aggregates.
        self.paxos_log: EventSourcedLog[PaxosLogged] = EventSourcedLog(
            self.events, uuid5(NAMESPACE_URL, "/paxoslog"), PaxosLogged
        )

    def propose_command(self, cmd_text: str) -> CommandFuture:
        # Parse the command text into a command object.
        cmd = self.command_class.parse(cmd_text)

        # Create a command future.
        future = CommandFuture(cmd_text=cmd_text)

        # Decide whether or not to put the command in the replicated log.
        if (self.log_read_commands or cmd.mutates_state) and self.num_participants > 1:

            # Start a Paxos instance.
            paxos_logged = self.paxos_log.trigger_event()
            proposal_key = self.create_paxos_aggregate_id_from_round(
                round=paxos_logged.originator_version
            )
            paxos_aggregate = self.start_paxos(
                proposal_key, cmd_text, self.assume_leader
            )

            # Put the future in cache, to result after reaching consensus.
            evicted_future = self.futures.put(proposal_key, future)

            # Finish any evicted future, with an error in anybody waiting.
            self.set_future_exception(
                evicted_future, CommandFutureEvicted("Futures cache full")
            )

            # Save the Paxos aggregate.
            try:
                self.save(paxos_aggregate, paxos_logged)

            # Conflicted with other replicas.
            except IntegrityError:
                msg = f"{self.name}: Rejecting command for round {paxos_logged.originator_version}"
                if list(self.events.get(paxos_aggregate.id)):
                    msg += f": Already have paxos aggregate: {paxos_aggregate.id}"
                error = CommandRejected(msg)

                # Set error on command future.
                self.set_future_exception(future, error)

                # Evict the future from the cache.
                self.get_command_future(paxos_aggregate.id, evict=True)

        else:
            # Just execute the command immediately and save results.
            try:
                aggregates, result = cmd.execute(self)
                self.save(*aggregates)
            except Exception as e:
                self.set_future_exception(future, CommandExecutionError(e))
            else:
                self.set_future_result(future, result)

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
        # Just process Paxos messages.
        if isinstance(domain_event, PaxosAggregate.MessageAnnounced):

            # Process Paxos message.
            print(self.name, domain_event.originator_id, "processing", domain_event.msg)
            try:
                paxos_aggregate, resolution_msg = self.process_message_announced(
                    domain_event
                )
            except Exception as e:
                # Re-raise a protocol implementation error.
                error_msg = f"{self.name} {domain_event.originator_id} errored processing {domain_event.msg}"
                raise PaxosProtocolError(error_msg) from e

            # Decide if we have a new Paxos aggregate.
            if len(paxos_aggregate.pending_events) == paxos_aggregate.version:
                # Add new paxos aggregate to the paxos log.
                paxos_logged = self.paxos_log.trigger_event()
                paxos_id = self.create_paxos_aggregate_id_from_round(
                    paxos_logged.originator_version
                )
                # Check the paxos log isn't out of order.
                if paxos_id != paxos_aggregate.id:
                    # Out of order commands (new command was proposed since starting Paxos).
                    future = self.get_command_future(paxos_aggregate.id, evict=True)
                    self.set_future_exception(
                        future, CommandRejected("Paxos log position now occupied")
                    )
                else:
                    # Collect events from Paxos aggregate and log.
                    process_event.collect_events(paxos_aggregate)
                    process_event.collect_events(paxos_logged)

            else:
                # Collect events from Paxos aggregate.
                process_event.collect_events(paxos_aggregate)

            # Decide if consensus has been reached on the command.
            if resolution_msg:
                aggregates, result = (), None
                # Parse the command text into a command object.
                cmd = self.command_class.parse(paxos_aggregate.final_value)

                # Decide if we will execute the command.
                future = self.get_command_future(paxos_aggregate.id, evict=True)
                if cmd.mutates_state or future:
                    print(self.name, "executing", paxos_aggregate.final_value)
                    try:
                        aggregates, result = cmd.execute(self)
                    except Exception as e:
                        # Set execution error on future.
                        self.set_future_exception(future, CommandExecutionError(e))
                    else:
                        # Collect key-value aggregate events.
                        process_event.collect_events(*aggregates)

                        # Check we executed what was expected by the client.
                        if (
                            future
                            and future.original_cmd_text != paxos_aggregate.final_value
                        ):
                            self.set_future_exception(
                                future,
                                CommandRejected(
                                    "Executed another command '{}'".format(
                                        paxos_aggregate.final_value
                                    )
                                ),
                            )
                        else:
                            # Set the command result on the command future.
                            self.set_future_result(future, result)

    def get_command_future(self, key: UUID, evict: bool = False):
        try:
            future = self.futures.get(key, evict=evict)
        except KeyError:
            future = None
        return future

    @staticmethod
    def set_future_exception(future: Optional[CommandFuture], error: Exception):
        if future:
            try:
                future.set_exception(error)
            except InvalidStateError:
                pass

    @staticmethod
    def set_future_result(future: Optional[CommandFuture], result: Any):
        if future:
            try:
                future.set_result(result)
            except InvalidStateError:
                pass

    # def record(self, process_event: ProcessEvent) -> Optional[int]:
    #     new_events = list(process_event.events)
    #     try:
    #         returning = super().record(process_event)
    #     except Exception:
    #         print(self.name, f"errored saving {len(process_event.events)} events")
    #
    #         for new_event in new_events:
    #             print(
    #                 self.name,
    #                 new_event.originator_id,
    #                 "errored saving event",
    #                 new_event.originator_id,
    #                 new_event.originator_version,
    #                 process_event.tracking
    #                 and process_event.tracking.notification_id
    #                 or None,
    #                 process_event.tracking
    #                 and process_event.tracking.application_name
    #                 or None,
    #             )
    #             if list(
    #                 self.recorder.select_events(
    #                     new_event.originator_id,
    #                     lte=new_event.originator_version,
    #                     gt=new_event.originator_version - 1,
    #                     limit=1,
    #                 )
    #             ):
    #                 print(
    #                     "Already have recorded event for",
    #                     new_event.originator_id,
    #                     new_event.originator_version,
    #                 )
    #             else:
    #                 print(
    #                     "Don't have recorded event for",
    #                     new_event.originator_id,
    #                     new_event.originator_version,
    #                 )
    #
    #         if process_event.tracking:
    #             max_tracking_id = self.recorder.max_tracking_id(
    #                 process_event.tracking.application_name
    #             )
    #             if max_tracking_id >= process_event.tracking.notification_id:
    #                 print("Recorded tracking ID greater than process event")
    #
    #         raise
    #     else:
    #         for new_event in new_events:
    #             print(
    #                 self.name,
    #                 new_event.originator_id,
    #                 "saved event",
    #                 new_event.originator_id,
    #                 new_event.originator_version,
    #                 process_event.tracking
    #                 and process_event.tracking.notification_id
    #                 or None,
    #                 process_event.tracking
    #                 and process_event.tracking.application_name
    #                 or None,
    #             )
    #     return returning

    def notify(self, new_events: List[AggregateEvent[Aggregate]]) -> None:
        """
        Extends the application :func:`~eventsourcing.application.Application.notify`
        method by calling :func:`prompt_followers` whenever new events have just
        been saved.
        """
        for new_event in new_events:
            if isinstance(new_event, PaxosAggregate.MessageAnnounced):
                print(
                    self.name,
                    new_event.originator_id,
                    "saved",
                    new_event.msg,
                    new_event.originator_id,
                    new_event.originator_version,
                )
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
