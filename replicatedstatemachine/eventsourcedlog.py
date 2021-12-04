from typing import Generic, Iterator, Optional, Type, cast
from uuid import UUID

from eventsourcing.domain import Aggregate, AggregateEvent, TDomainEvent
from eventsourcing.persistence import EventStore


class EventSourcedLog(Generic[TDomainEvent]):
    def __init__(
        self,
        events: EventStore[AggregateEvent[Aggregate]],
        originator_id: UUID,
        logged_cls: Type[TDomainEvent],
    ):
        self.events = events
        self.originator_id = originator_id
        self.logged_cls = logged_cls

    def trigger_event(
        self, next_originator_version: Optional[int], **kwargs
    ) -> TDomainEvent:
        if next_originator_version is None:
            last_logged = self.get_last()
            if last_logged:
                next_originator_version = last_logged.originator_version + 1
            else:
                next_originator_version = Aggregate.INITIAL_VERSION
        return self.logged_cls(
            originator_id=self.originator_id,
            originator_version=next_originator_version,
            timestamp=self.logged_cls.create_timestamp(),
            **kwargs
        )

    def get_last(self) -> Optional[TDomainEvent]:
        # Get last logged event.
        try:
            return next(self.get(desc=True, limit=1))
        except StopIteration:
            return None

    def get(
        self,
        gt: Optional[int] = None,
        lte: Optional[int] = None,
        desc: bool = False,
        limit: Optional[int] = None,
    ) -> Iterator[TDomainEvent]:
        # Get logged events.
        return cast(
            Iterator[TDomainEvent],
            self.events.get(
                originator_id=self.originator_id,
                gt=gt,
                lte=lte,
                desc=desc,
                limit=limit,
            ),
        )
