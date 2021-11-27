from __future__ import annotations

from unittest import TestCase

from kvstore.domainmodel import HashAggregate


class TestKVAggregates(TestCase):
    def test_hash(self):
        a = HashAggregate("myhash")
        self.assertEqual(a.key_name, "myhash")
        self.assertEqual(a.get_field_value("field"), None)
        a.set_field_value("field", "value")
        self.assertEqual(a.get_field_value("field"), "value")
