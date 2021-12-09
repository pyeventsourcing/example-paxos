from eventsourcing.domain import Aggregate, AggregateEvent

from paxossystem.domainmodel import PaxosAggregate


class PaxosLogged(AggregateEvent[Aggregate]):
    pass


class CommandForwarded(AggregateEvent[Aggregate]):
    cmd_text: str
    elected_leader: str


class LeadershipElection(PaxosAggregate):
    pass
