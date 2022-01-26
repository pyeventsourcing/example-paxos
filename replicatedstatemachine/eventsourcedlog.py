from typing import Generic, Iterator, Optional, Type, cast
from uuid import UUID

from eventsourcing.domain import Aggregate, AggregateEvent, TDomainEvent
from eventsourcing.persistence import EventStore
