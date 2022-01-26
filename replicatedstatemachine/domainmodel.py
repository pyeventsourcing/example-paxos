from eventsourcing.domain import Aggregate, AggregateEvent, LogEvent

from paxossystem.domainmodel import PaxosAggregate


class LeadershipElection(PaxosAggregate):
    pass


class ElectionLogged(LogEvent):
    pass


class CommandProposal(PaxosAggregate):
    pass


class CommandLogged(LogEvent):
    pass


class CommandForwarded(AggregateEvent[Aggregate]):
    cmd_text: str
    elected_leader: str
