from copy import deepcopy
from threading import RLock
from typing import Dict, Generic, Optional, TypeVar
from uuid import UUID

from eventsourcing.application import (
    Application,
    ProcessEvent,
    ProjectorFunctionType,
    Repository,
    mutate_aggregate,
)
from eventsourcing.domain import AggregateEvent, Snapshot, TAggregate
from eventsourcing.persistence import EventStore
from eventsourcing.utils import strtobool

S = TypeVar("S")
T = TypeVar("T")


class Cache(Generic[S, T]):
    def __init__(self):
        self.cache: Dict[S, T] = {}

    def get(self, key: S) -> T:
        return self.cache[key]

    def put(self, key: S, value: T) -> None:
        if value is not None:
            self.cache[key] = value


class LRUCache(Cache[S, T]):
    """
    Size limited caching that tracks accesses by recency.

    This is basically copied from functools.lru_cache. But
    we need to know when there was a cache hit so we can
    fast-forward the aggregate with new stored events.
    """

    sentinel = object()  # unique object used to signal cache misses
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3  # names for the link fields

    def __init__(self, maxsize: int):
        # Constants shared by all lru cache instances:
        super().__init__()
        self.maxsize = maxsize
        self.full = False
        self.lock = RLock()  # because linkedlist updates aren't threadsafe
        self.root = []  # root of the circular doubly linked list
        self.clear()

    def clear(self):
        self.root[:] = [
            self.root,
            self.root,
            None,
            None,
        ]  # initialize by pointing to self

    def get(self, key: S) -> T:
        with self.lock:
            link = self.cache.get(key)
            if link is not None:
                # Move the link to the front of the circular queue.
                link_prev, link_next, _key, result = link
                link_prev[LRUCache.NEXT] = link_next
                link_next[LRUCache.PREV] = link_prev
                last = self.root[LRUCache.PREV]
                last[LRUCache.NEXT] = self.root[LRUCache.PREV] = link
                link[LRUCache.PREV] = last
                link[LRUCache.NEXT] = self.root
                return result
            else:
                raise KeyError

    def put(self, key: S, value: T) -> None:
        with self.lock:
            link = self.cache.get(key)
            if link is not None:
                # Set value.
                link[LRUCache.RESULT] = value
                # Move the link to the front of the circular queue.
                link_prev, link_next, _key, result = link
                link_prev[LRUCache.NEXT] = link_next
                link_next[LRUCache.PREV] = link_prev
                last = self.root[LRUCache.PREV]
                last[LRUCache.NEXT] = self.root[LRUCache.PREV] = link
                link[LRUCache.PREV] = last
                link[LRUCache.NEXT] = self.root
            elif self.full:
                # Use the old root to store the new key and result.
                oldroot = self.root
                oldroot[LRUCache.KEY] = key
                oldroot[LRUCache.RESULT] = value
                # Empty the oldest link and make it the new root.
                # Keep a reference to the old key and old result to
                # prevent their ref counts from going to zero during the
                # update. That will prevent potentially arbitrary object
                # clean-up code (i.e. __del__) from running while we're
                # still adjusting the links.
                self.root = oldroot[LRUCache.NEXT]
                oldkey = self.root[LRUCache.KEY]
                _ = self.root[LRUCache.RESULT]
                self.root[LRUCache.KEY] = self.root[LRUCache.RESULT] = None
                # Now update the cache dictionary.
                del self.cache[oldkey]
                # Save the potentially reentrant cache[key] assignment
                # for last, after the root and links have been put in
                # a consistent state.
                self.cache[key] = oldroot
            else:
                # Put result in a new link at the front of the queue.
                last = self.root[LRUCache.PREV]
                link = [last, self.root, key, value]
                last[LRUCache.NEXT] = self.root[LRUCache.PREV] = self.cache[key] = link
                # Use the __len__() bound method instead of the len() function
                # which could potentially be wrapped in an lru_cache itself.
                self.full = self.cache.__len__() >= self.maxsize


class CachedRepository(Repository[TAggregate]):
    def __init__(
        self,
        event_store: EventStore[AggregateEvent[TAggregate]],
        snapshot_store: Optional[EventStore[Snapshot[TAggregate]]] = None,
        cache_maxsize: Optional[int] = None,
        fastforward: bool = True,
    ):
        super().__init__(event_store, snapshot_store)
        if cache_maxsize is None:
            self.cache: Optional[Cache[UUID, TAggregate]] = None
        elif cache_maxsize <= 0:
            self.cache = Cache()
        else:
            self.cache = LRUCache(maxsize=cache_maxsize)
        self.fastforward = fastforward

    def get(
        self,
        aggregate_id: UUID,
        version: Optional[int] = None,
        projector_func: ProjectorFunctionType[TAggregate] = mutate_aggregate,
    ) -> TAggregate:
        if self.cache and version is None:
            try:
                # Look for aggregate in the cache.
                aggregate = self.cache.get(aggregate_id)
            except KeyError:
                # Reconstruct aggregate from stored events.
                aggregate = super().get(aggregate_id, projector_func=mutate_aggregate)
                # Put aggregate in the cache.
                self.cache.put(aggregate_id, aggregate)
            else:
                if self.fastforward:
                    # Fast forward cached aggregate.
                    new_events = self.event_store.get(
                        originator_id=aggregate_id, gt=aggregate.version
                    )
                    aggregate = mutate_aggregate(aggregate, new_events)
            # Deep copy cached aggregate, so bad mutations don't corrupt cache.
            aggregate = deepcopy(aggregate)
        else:
            # Reconstruct historical version of aggregate from stored events.
            aggregate = super().get(
                aggregate_id, version=version, projector_func=mutate_aggregate
            )
        return aggregate


class CachingApplication(Application[TAggregate]):
    AGGREGATE_CACHE_MAXSIZE = "AGGREGATE_CACHE_MAXSIZE"
    AGGREGATE_CACHE_FASTFORWARD = "AGGREGATE_CACHE_FASTFORWARD"

    def construct_repository(self) -> CachedRepository[TAggregate]:
        return CachedRepository(
            event_store=self.events,
            snapshot_store=self.snapshots,
            cache_maxsize=self.env.get(self.AGGREGATE_CACHE_MAXSIZE),
            fastforward=strtobool(self.env.get(self.AGGREGATE_CACHE_FASTFORWARD, "y")),
        )

    def record(self, process_event: ProcessEvent) -> Optional[int]:
        returning = super().record(process_event)
        for aggregate_id, aggregate in process_event.aggregates.items():
            if self.repository.cache:
                self.repository.cache.put(aggregate_id, aggregate)
        return returning