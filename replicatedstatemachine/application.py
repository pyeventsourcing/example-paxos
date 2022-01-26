import random
from abc import ABC, abstractmethod
from concurrent.futures import InvalidStateError
from random import random, shuffle
from threading import Timer
from typing import (
    Any,
    Mapping,
    Optional,
    Tuple,
    Type,
)
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from eventsourcing.application import Cache, LRUCache, ProcessingEvent, EventSourcedLog
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import IntegrityError
from eventsourcing.utils import get_topic, resolve_topic

from paxossystem.composable import Nack, Promise
from replicatedstatemachine.commandfutures import CommandFuture
from replicatedstatemachine.domainmodel import (
    CommandForwarded,
    CommandProposal, ElectionLogged,
    LeadershipElection,
    CommandLogged,
)
from replicatedstatemachine.exceptions import (
    CommandExecutionError,
    PaxosProtocolError,
    CommandEvictedError,
    CommandRejectedError,
)
from paxossystem.application import PaxosApplication


class StateMachineReplica(PaxosApplication):
    COMMAND_CLASS = "COMMAND_CLASS"
    FUTURES_CACHE_MAXSIZE = "FUTURES_CACHE_MAXSIZE"
    pull_section_size = 100
    log_section_size = 100
    futures_cache_maxsize = 5000
    log_read_commands = True
    leader_lease = 10

    follow_topics = notify_topics = [
        get_topic(LeadershipElection.MessageAnnounced),
        get_topic(CommandProposal.MessageAnnounced),
        get_topic(CommandForwarded),
    ]

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

        # Construct log of leadership election aggregates.
        self.election_log: EventSourcedLog[ElectionLogged] = EventSourcedLog(
            self.events, uuid5(NAMESPACE_URL, "/election_log"), ElectionLogged
        )
        # Construct log of command proposal aggregates.
        self.command_log: EventSourcedLog[CommandLogged] = EventSourcedLog(
            self.events, uuid5(NAMESPACE_URL, "/command_log"), CommandLogged
        )
        self.next_leadership_election_timer = None
        self.disestablish_leadership_timer = None
        self.elected_leader_uid = None
        self.probability_leader_fails_to_propose_reelection = 0

    @property
    def is_elected_leader(self) -> bool:
        return self.elected_leader_uid == self.name

    def close(self) -> None:
        with self.processing_lock:
            if self.next_leadership_election_timer:
                self.next_leadership_election_timer.cancel()
            if self.disestablish_leadership_timer:
                self.disestablish_leadership_timer.cancel()
        super().close()

    def disestablish_leadership(self):
        self.elected_leader_uid = None

    def propose_reelection(self, election_round: int):
        if random() >= self.probability_leader_fails_to_propose_reelection:
            self.propose_leader(election_round)

    def propose_leader(self, election_round=None):
        with self.processing_lock:
            if self.closing.is_set():
                return
            highest_recorded = self.get_highest_recorded_election_round()
            if election_round is None:
                election_round = highest_recorded + 1
            if election_round > highest_recorded:
                print(self.name, "proposing leadership, election round", election_round)
                proposal_key = create_leadership_proposal_id_from_round(
                    round=election_round
                )
                if proposal_key not in self.repository:
                    # Don't believe there is an election underway, so start one.
                    proposal_delays = list(range(1, self.num_participants + 1))
                    shuffle(proposal_delays)
                    paxos_aggregate = self.start_paxos(
                        key=proposal_key,
                        value=[self.name, election_round, proposal_delays],
                        assume_leader=False,
                        paxos_cls=LeadershipElection,
                    )
                    self.save(paxos_aggregate)
                    return
        print(self.name, "not proposing leadership, election round", election_round)

    def propose_command(self, cmd_text: str) -> CommandFuture:
        # Parse the command text into a command object.
        cmd = self.command_class.parse(cmd_text)

        # Create a command future.
        future = CommandFuture(cmd_text=cmd_text)

        # Decide whether or not to put the command in the replicated log.
        if (self.log_read_commands or cmd.mutates_state) and self.num_participants > 1:

            if self.elected_leader_uid and not self.is_elected_leader:
                with self.processing_lock:
                    # Forward the command to the elected leader.
                    forwarded_command = CommandForwarded(
                        originator_id=uuid4(),
                        originator_version=Aggregate.INITIAL_VERSION,
                        timestamp=CommandForwarded.create_timestamp(),
                        cmd_text=cmd_text,
                        elected_leader=self.elected_leader_uid,
                    )
                    # Put the command future in cache, for setting results later.
                    self.cache_command_future(forwarded_command.originator_id, future)
                    # Save the forwarded command.
                    self.save(forwarded_command)

            else:
                # Propose the command.
                with self.processing_lock:
                    # Create and log a Paxos aggregate.
                    command_logged = self.command_log.trigger_event()
                    proposal_key = create_command_proposal_id_from_round(
                        round=command_logged.originator_version
                    )
                    command_proposal = self.start_paxos(
                        key=proposal_key,
                        value=[cmd_text, None],
                        assume_leader=self.assume_leader or self.is_elected_leader,
                        paxos_cls=CommandProposal,
                    )

                    # Put the future in cache, set result after reaching consensus.
                    self.cache_command_future(proposal_key, future)

                    # Save the Paxos aggregate and logged event.
                    try:
                        self.save(command_proposal, command_logged)

                    except IntegrityError:
                        # Aggregate or logged event already exists.
                        msg = f"{self.name}: Rejecting command for round {command_logged.originator_version}"
                        if list(self.events.get(command_proposal.id)):
                            msg += (
                                f": Already have paxos aggregate: {command_proposal.id}"
                            )
                        error = CommandRejectedError(msg)

                        # Set error on command future.
                        self.set_future_exception(future, error)

                        # Evict the future from the cache.
                        self.get_command_future(command_proposal.id, evict=True)

        else:
            # Execute the command immediately and save results.
            try:
                with self.processing_lock:
                    aggregates, result = cmd.execute(self)
                    self.save(*aggregates)
            except Exception as e:
                self.set_future_exception(future, CommandExecutionError(e))
            else:
                self.set_future_result(future, result)

        # Return the command future, so clients can wait for results.
        return future

    def policy(
        self,
        domain_event: AggregateEvent[TAggregate],
        process_event: ProcessingEvent,
    ) -> None:
        """
        Processes paxos "message announced" events of other applications
        by starting or continuing a paxos aggregate in this application.
        """
        if isinstance(domain_event, CommandForwarded):
            # Was this command forwarded to us?
            if domain_event.elected_leader == self.name:
                if self.elected_leader_uid and not self.is_elected_leader:
                    print(f"Forwarding forwarded command from {self.name} to leader {self.elected_leader_uid}")
                    # Forward it to the elected leader.
                    command_forwarded = CommandForwarded(
                        originator_id=domain_event.originator_id,
                        originator_version=domain_event.originator_version + 1,
                        timestamp=CommandForwarded.create_timestamp(),
                        cmd_text=domain_event.cmd_text,
                        elected_leader=self.elected_leader_uid,
                    )
                    process_event.collect_events(command_forwarded)
                else:
                    if self.is_elected_leader:
                        pass
                        # print("Proposing forwarded command assuming leadership")
                    else:
                        print(f"{self.name} Proposing forwarded command without assuming leadership")

                    # Start a Paxos instance assuming leadership.
                    paxos_logged = self.command_log.trigger_event()
                    proposal_key = create_command_proposal_id_from_round(
                        round=paxos_logged.originator_version
                    )
                    command_proposal = self.start_paxos(
                        key=proposal_key,
                        value=[domain_event.cmd_text, domain_event.originator_id],
                        assume_leader=self.is_elected_leader,
                        paxos_cls=CommandProposal,
                    )
                    process_event.collect_events(command_proposal, paxos_logged)

        elif isinstance(domain_event, LeadershipElection.MessageAnnounced):
            if (
                isinstance(domain_event.msg, (Promise, Nack))
                and domain_event.msg.proposal_id.uid != self.name
            ):
               return

            leadership_election, resolution_msg = self.process_announced_message(
                domain_event, LeadershipElection
            )
            process_event.collect_events(leadership_election)
            if resolution_msg:
                election_round = leadership_election.final_value[1]
                proposal_delays = leadership_election.final_value[2]
                highest_recorded_election_round = self.get_highest_recorded_election_round()
                if election_round > highest_recorded_election_round:

                    if self.disestablish_leadership_timer:
                        self.disestablish_leadership_timer.cancel()

                    if self.next_leadership_election_timer:
                        self.next_leadership_election_timer.cancel()

                    # Set elected leader.
                    self.elected_leader_uid = leadership_election.final_value[0]

                    election_logged = self.election_log.trigger_event(election_round)
                    next_election_round = election_round + 1
                    process_event.collect_events(election_logged)

                    if self.is_elected_leader:
                        print(self.name, "elected leader for round", election_round, "in",
                              leadership_election.modified_on -
                              leadership_election.created_on)
                        self.next_leadership_election_timer = Timer(
                            self.leader_lease,
                            self.propose_reelection,
                            args=(next_election_round,),
                        )
                        self.next_leadership_election_timer.daemon = True
                        self.next_leadership_election_timer.start()
                    else:
                        self.next_leadership_election_timer = Timer(
                            self.leader_lease + 1 + proposal_delays[int(self.name[-1])],
                            self.propose_leader,
                            args=(next_election_round,),
                        )
                        self.next_leadership_election_timer.daemon = True
                        self.next_leadership_election_timer.start()

                    self.disestablish_leadership_timer = Timer(
                        self.leader_lease + 1,
                        self.disestablish_leadership,
                    )
                    self.disestablish_leadership_timer.daemon = True
                    self.disestablish_leadership_timer.start()


        elif isinstance(domain_event, CommandProposal.MessageAnnounced):

            # Process Paxos message.
            try:
                command_proposal, resolution_msg = self.process_announced_message(
                    domain_event=domain_event, paxos_cls=CommandProposal
                )
            except Exception as e:
                # Re-raise a protocol implementation error.
                error_msg = f"{self.name} {domain_event.originator_id} errored processing {domain_event.msg}: {e}"
                raise PaxosProtocolError(error_msg) from e

            # Decide if we have a new Paxos aggregate.
            if len(command_proposal.pending_events) == command_proposal.version:
                # Add new paxos aggregate to the paxos log.
                paxos_logged = self.command_log.trigger_event()
                expected_paxos_id = create_command_proposal_id_from_round(
                    paxos_logged.originator_version
                )
                # Check the paxos log order.
                if expected_paxos_id == command_proposal.id:
                    # Collect events from Paxos aggregate and log.
                    process_event.collect_events(command_proposal)
                    process_event.collect_events(paxos_logged)
                else:
                    # Out of order commands (new command was proposed since starting Paxos).
                    future = self.get_command_future(command_proposal.id, evict=True)
                    self.set_future_exception(
                        future, CommandRejectedError("Paxos log position now occupied")
                    )
            else:
                # Collect events from Paxos aggregate.
                process_event.collect_events(command_proposal)

            # Decide if consensus has been reached on the command.
            if resolution_msg:
                # Parse the command text into a command object.
                cmd_text = command_proposal.final_value[0]
                cmd_id = command_proposal.final_value[1]
                cmd = self.command_class.parse(cmd_text)

                # Decide if we will execute the command.
                future_key = cmd_id or command_proposal.id
                future = self.get_command_future(future_key, evict=True)
                if cmd.mutates_state or future:
                    try:
                        aggregates, result = cmd.execute(self)
                    except Exception as e:
                        # Set execution error on future.
                        self.set_future_exception(future, CommandExecutionError(e))
                    else:
                        # Collect key-value aggregate events.
                        process_event.collect_events(*aggregates)

                        # Check we executed what was expected by the client.
                        if future:
                            if future.original_cmd_text != cmd_text:

                                # Are we still the elected leader?
                                future.reproposal_count += 1
                                if self.elected_leader_uid and not self.is_elected_leader:
                                    print(f"{self.name} forwarding rejected command to leadership")
                                    # Forward it to the new elected leader.
                                    command_forwarded = CommandForwarded(
                                        originator_id=uuid4(),
                                        originator_version=Aggregate.INITIAL_VERSION,
                                        timestamp=CommandForwarded.create_timestamp(),
                                        cmd_text=future.original_cmd_text,
                                        elected_leader=self.elected_leader_uid,
                                    )
                                    process_event.collect_events(command_forwarded)
                                    self.cache_command_future(command_forwarded.originator_id, future)
                                else:
                                    if self.is_elected_leader:
                                        print(f"{self.name} reproposing rejected command assuming leadership")
                                    else:
                                        print(f"{self.name} reproposing rejected command without assuming leadership")
                                    # Start a Paxos instance assuming leadership.
                                    paxos_logged = self.command_log.trigger_event()
                                    proposal_key = create_command_proposal_id_from_round(
                                        round=paxos_logged.originator_version
                                    )
                                    command_proposal = self.start_paxos(
                                        key=proposal_key,
                                        value=[future.original_cmd_text, domain_event.originator_id],
                                        assume_leader=self.is_elected_leader,
                                        paxos_cls=CommandProposal,
                                    )
                                    process_event.collect_events(command_proposal, paxos_logged)
                                    self.cache_command_future(command_proposal.id, future)


                                # self.set_future_exception(
                                #     future,
                                #     CommandRejectedError(
                                #         "Executed another command '{}'".format(cmd_text)
                                #     ),
                                # )
                            else:
                                # Set the command result on the command future.
                                if future and future.reproposal_count:
                                    print(f"Completed reproposed command: {future.original_cmd_text}")
                                self.set_future_result(future, result)

    def cache_command_future(self, proposal_key, future):
        # Put the future in the cache.
        evicted_key, evicted_future = self.futures.put(proposal_key, future)

        # Finish any evicted future, with an error in anybody waiting.
        msg = f"Evicted because oldest in futures cache, key {evicted_key}"
        self.set_future_exception(evicted_future, CommandEvictedError(msg))

    def get_command_future(self, key: UUID, evict: bool = False) -> Optional[CommandFuture]:
        try:
            future = self.futures.get(key, evict=evict)
        except KeyError:
            future = None
        return future

    @staticmethod
    def set_future_result(future: Optional[CommandFuture], result: Any):
        if future:
            try:
                future.set_result(result)
            except InvalidStateError:
                pass

    @staticmethod
    def set_future_exception(future: Optional[CommandFuture], error: Exception):
        if future:
            try:
                future.set_exception(error)
            except InvalidStateError:
                pass

    def get_highest_recorded_election_round(self) -> int:
        last_logged = self.election_log.get_last()
        if last_logged is not None:
            return last_logged.originator_version
        else:
            return 0


def create_command_proposal_id_from_round(round: int) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"/commands/{round}",
    )


def create_leadership_proposal_id_from_round(round: int) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"/elections/{round}",
    )


class Command(ABC):
    mutates_state = True

    @classmethod
    @abstractmethod
    def parse(cls, cmd_text: str) -> "Command":
        pass

    @abstractmethod
    def execute(self, app: StateMachineReplica) -> Tuple[Tuple[Aggregate, ...], Any]:
        pass
