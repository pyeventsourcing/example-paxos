from eventsourcing.domain import Aggregate, AggregateEvent

from paxossystem.domainmodel import PaxosAggregate


class LeadershipElection(PaxosAggregate):
    pass


class ElectionLogged(AggregateEvent[Aggregate]):
    pass


class CommandProposal(PaxosAggregate):
    pass


class CommandLogged(AggregateEvent[Aggregate]):
    pass


class CommandForwarded(AggregateEvent[Aggregate]):
    cmd_text: str
    elected_leader: str
