from eventsourcing.domain import Aggregate, AggregateEvent


class PaxosLogged(AggregateEvent[Aggregate]):
    pass
