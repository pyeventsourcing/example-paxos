from eventsourcing.domain import Aggregate, AggregateEvent


class PaxosLogged(AggregateEvent[Aggregate]):
    pass


class CommandForwarded(AggregateEvent[Aggregate]):
    cmd_text: str
