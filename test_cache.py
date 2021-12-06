from unittest import TestCase

from paxossystem.cache import LRUCache


class TestLRUCache(TestCase):
    def test_put_get(self):
        cache = LRUCache(maxsize=2)

        with self.assertRaises(KeyError):
            cache.get(1)

        evicted = cache.put(1, 1)
        self.assertEqual(None, evicted)
        self.assertEqual(1, cache.get(1))

        evicted = cache.put(2, 2)
        self.assertEqual(None, evicted)
        self.assertEqual(1, cache.get(1))
        self.assertEqual(2, cache.get(2))

        evicted = cache.put(3, 3)
        self.assertEqual(1, evicted)

        self.assertEqual(3, cache.get(3))
        self.assertEqual(2, cache.get(2))

        with self.assertRaises(KeyError):
            cache.get(1)

    def test_put_get_evict_recent(self):
        cache = LRUCache(maxsize=3)

        cache.put(1, 1)
        self.assertEqual(1, cache.get(1))
        self.assertEqual(1, cache.get(1))
        self.assertEqual(1, cache.get(1, evict=True))

        with self.assertRaises(KeyError):
            self.assertEqual(1, cache.get(1))

        cache.put(1, 1)
        cache.put(2, 2)
        cache.put(3, 3)
        evicted = cache.put(4, 4)
        self.assertEqual(1, evicted)

        self.assertEqual(3, cache.get(3, evict=True))

        evicted = cache.put(5, 5)
        self.assertEqual(None, evicted)

        evicted = cache.put(6, 6)
        self.assertEqual(2, evicted)

        evicted = cache.put(7, 7)
        self.assertEqual(4, evicted)

    def test_put_get_evict_oldest(self):
        cache = LRUCache(maxsize=3)

        cache.put(1, 1)
        cache.put(2, 2)
        cache.put(3, 3)
        self.assertEqual(1, cache.get(1, evict=True))

        evicted = cache.put(4, 4)
        self.assertEqual(None, evicted)

        evicted = cache.put(5, 5)
        self.assertEqual(2, evicted)

        evicted = cache.put(6, 6)
        self.assertEqual(3, evicted)

        evicted = cache.put(7, 7)
        self.assertEqual(4, evicted)

    def test_put_get_evict_newest(self):
        cache = LRUCache(maxsize=3)

        cache.put(1, 1)
        cache.put(2, 2)
        cache.put(3, 3)
        self.assertEqual(3, cache.get(3, evict=True))

        evicted = cache.put(4, 4)
        self.assertEqual(None, evicted)

        evicted = cache.put(5, 5)
        self.assertEqual(1, evicted)

        evicted = cache.put(6, 6)
        self.assertEqual(2, evicted)

        evicted = cache.put(7, 7)
        self.assertEqual(4, evicted)
