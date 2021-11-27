from threading import RLock

from eventsourcing.application import Repository
from eventsourcing.domain import TAggregate


class CachedRepository(Repository):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = Cache(maxsize=500)

    def get(self, aggregate_id) -> TAggregate:
        try:
            return self.cache.get(aggregate_id)
        except KeyError:
            aggregate = super().get(aggregate_id)
            self.cache.put(aggregate_id, aggregate)
            return aggregate


class Cache:
    """
    This is basically copied from functools.lru_cache. But
    we need to have a put() method, so needed to write this.
    """

    sentinel = object()  # unique object used to signal cache misses
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3  # names for the link fields

    def __init__(self, maxsize):
        # Constants shared by all lru cache instances:

        self.maxsize = maxsize
        self.cache = {}
        self.full = False
        self.lock = RLock()  # because linkedlist updates aren't threadsafe
        self.root = []  # root of the circular doubly linked list
        self.root[:] = [
            self.root,
            self.root,
            None,
            None,
        ]  # initialize by pointing to self

    def get(self, key):
        # Size limited caching that tracks accesses by recency
        with self.lock:
            link = self.cache.get(key)
            if link is not None:
                # Move the link to the front of the circular queue
                link_prev, link_next, _key, result = link
                link_prev[Cache.NEXT] = link_next
                link_next[Cache.PREV] = link_prev
                last = self.root[Cache.PREV]
                last[Cache.NEXT] = self.root[Cache.PREV] = link
                link[Cache.PREV] = last
                link[Cache.NEXT] = self.root
                return result
            else:
                raise KeyError

    def put(self, key, value):
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
                oldroot[Cache.KEY] = key
                oldroot[Cache.RESULT] = value
                # Empty the oldest link and make it the new root.
                # Keep a reference to the old key and old result to
                # prevent their ref counts from going to zero during the
                # update. That will prevent potentially arbitrary object
                # clean-up code (i.e. __del__) from running while we're
                # still adjusting the links.
                self.root = oldroot[Cache.NEXT]
                oldkey = self.root[Cache.KEY]
                _ = self.root[Cache.RESULT]
                self.root[Cache.KEY] = self.root[Cache.RESULT] = None
                # Now update the cache dictionary.
                del self.cache[oldkey]
                # Save the potentially reentrant cache[key] assignment
                # for last, after the root and links have been put in
                # a consistent state.
                self.cache[key] = oldroot
            else:
                # Put result in a new link at the front of the queue.
                last = self.root[Cache.PREV]
                link = [last, self.root, key, value]
                last[Cache.NEXT] = self.root[Cache.PREV] = self.cache[key] = link
                # Use the cache_len bound method instead of the len() function
                # which could potentially be wrapped in an lru_cache itself.
                self.full = self.cache.__len__() >= self.maxsize
