from copy import deepcopy
from threading import RLock
from typing import Any, Dict, Generic, Optional, TypeVar
from uuid import UUID

from eventsourcing.application import (
    ProjectorFunctionType,
    Repository,
    mutate_aggregate,
)
from eventsourcing.domain import AggregateEvent, Snapshot, TAggregate
from eventsourcing.persistence import EventStore


class CachedRepository(Repository[TAggregate]):
    def __init__(
        self,
        event_store: EventStore[AggregateEvent[TAggregate]],
        snapshot_store: Optional[EventStore[Snapshot[TAggregate]]] = None,
        cache_size: Optional[int] = None,
    ):
        super().__init__(event_store, snapshot_store)
        if cache_size is None:
            self.cache: Optional[Cache] = None
        if isinstance(cache_size, int):
            if cache_size > 1:
                self.cache = LRUCache(maxsize=cache_size)
            else:
                self.cache = Cache()
        else:
            self.cache = None

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


T = TypeVar("T")


class Cache(Generic[T]):
    def __init__(self):
        self.cache: Dict[Any, T] = {}

    def get(self, key: Any) -> T:
        return self.cache.get(key)

    def put(self, key: Any, value: T) -> None:
        self.cache[key] = value


class LRUCache(Cache[T]):
    """
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

    def get(self, key: Any) -> T:
        # Size limited caching that tracks accesses by recency
        with self.lock:
            link = self.cache.get(key)
            if link is not None:
                # Move the link to the front of the circular queue
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

    def put(self, key: Any, value: T) -> None:
        with self.lock:
            if key in self.cache:
                # Getting here means that this same key was added to the
                # cache while the lock was released.  Since the link
                # update is already done, we need only return the
                # computed result and update the count of misses.
                pass
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
                # Use the cache_len bound method instead of the len() function
                # which could potentially be wrapped in an lru_cache itself.
                self.full = self.cache.__len__() >= self.maxsize
