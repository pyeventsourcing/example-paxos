from __future__ import annotations

from unittest import TestCase
from uuid import uuid4

from keyvaluestore.domainmodel import KeyValueAggregate


class TestKVAggregates(TestCase):
    def test_hash_methods(self):
        originator_id = uuid4()
        a = KeyValueAggregate(id=originator_id, key_name="myhash")
        self.assertEqual(a.id, originator_id)
        self.assertEqual(a.key_name, "myhash")
        self.assertEqual(a.get_field_value("field"), None)
        a.set_field_value("field", "value")
        self.assertEqual(a.get_field_value("field"), "value")
        a.del_field_value("field")
        self.assertEqual(a.get_field_value("field"), None)
