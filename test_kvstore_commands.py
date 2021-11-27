from unittest import TestCase

from kvstore.application import KVStore, execute_proposal, propose_command, split
from kvstore.domainmodel import HashAggregate
from kvstore.exceptions import AggregateVersionMismatch


class TestHashCommands(TestCase):
    def setUp(self) -> None:
        self.app = KVStore()

    def test_hset_command(self):
        # Propose command to set field.
        proposal = propose_command(self.app, 'HSET myhash field "value"')

        # Execute proposal.
        hash_aggregate = execute_proposal(self.app, proposal.value)

        # Check field has been set.
        self.assertIsInstance(hash_aggregate, HashAggregate)
        self.assertEqual(hash_aggregate.get_field_value("field"), "value")

        # Save the hash aggregate.

        # Check we can't execute same proposal twice.
        self.app.save(hash_aggregate)
        with self.assertRaises(AggregateVersionMismatch):
            execute_proposal(self.app, proposal.value)

        # Propose and execute command to update field.
        proposal = propose_command(self.app, 'HSET myhash field "value2"')
        hash_aggregate = execute_proposal(self.app, proposal.value)

        # Check the field has been updated.
        self.assertIsInstance(hash_aggregate, HashAggregate)
        self.assertEqual(hash_aggregate.get_field_value("field"), "value2")

        # Check we can't do it twice.
        self.app.save(hash_aggregate)
        with self.assertRaises(AggregateVersionMismatch):
            execute_proposal(self.app, proposal.value)

    def test_hget_command(self):
        # Save a hash aggregate.
        hash_aggregate = HashAggregate("myhash")
        hash_aggregate.set_field_value("field", "value")
        self.app.save(hash_aggregate)

        # Check we can get the field value.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

    def test_hdel_command(self):
        # Save a hash aggregate.
        hash_aggregate = HashAggregate("myhash")
        hash_aggregate.set_field_value("field", "value")
        self.app.save(hash_aggregate)

        # Propose and execute command to del field.
        proposal = propose_command(self.app, "HDEL myhash field")
        hash_aggregate = execute_proposal(self.app, proposal.value)

        # Check the field has been deleted.
        self.assertIsInstance(hash_aggregate, HashAggregate)
        self.assertEqual(hash_aggregate.get_field_value("field"), None)

    def test_hsetnx_command(self):
        # Save a hash aggregate.
        hash_aggregate = HashAggregate("myhash")
        hash_aggregate.set_field_value("field", "value")
        self.app.save(hash_aggregate)

        # Propose and execute command to del field.
        proposal = propose_command(self.app, "HSETNX myhash field value2")
        hash_aggregate = execute_proposal(self.app, proposal.value)

        # Check existing field has not updated.
        self.assertIsInstance(hash_aggregate, HashAggregate)
        self.assertEqual(hash_aggregate.get_field_value("field"), "value")

        # Propose and execute command to del field.
        proposal = propose_command(self.app, "HSETNX myhash field2 value2")
        hash_aggregate = execute_proposal(self.app, proposal.value)

        # Check the new field has been set.
        self.assertIsInstance(hash_aggregate, HashAggregate)
        self.assertEqual(hash_aggregate.get_field_value("field2"), "value2")

    def test_hincrby_command(self):
        # Save a hash aggregate.
        hash_aggregate = HashAggregate("myhash")
        hash_aggregate.set_field_value("field", "2")
        self.app.save(hash_aggregate)

        # Propose and execute command to increment field.
        proposal = propose_command(self.app, "HINCRBY myhash field 5")
        hash_aggregate = execute_proposal(self.app, proposal.value)

        # Check existing field has been incremented.
        self.assertIsInstance(hash_aggregate, HashAggregate)
        self.assertEqual(hash_aggregate.get_field_value("field"), "7")

        # Propose and execute command to decrement field.
        self.app.save(hash_aggregate)
        proposal = propose_command(self.app, "HINCRBY myhash field -3")
        hash_aggregate = execute_proposal(self.app, proposal.value)

        # Check existing field has been decremented.
        self.assertIsInstance(hash_aggregate, HashAggregate)
        self.assertEqual(hash_aggregate.get_field_value("field"), "4")

        # Check we can't do it twice.
        self.app.save(hash_aggregate)
        with self.assertRaises(AggregateVersionMismatch):
            execute_proposal(self.app, proposal.value)


class TestSplit(TestCase):
    good_examples = [
        ('this is "a test"', ["this", "is", "a test"]),
        ("this is 'a test'", ["this", "is", "a test"]),
        ('this "is a" test', ["this", "is a", "test"]),
        ('this "is "a test', ["this", "is a", "test"]),  # ?
        ('this "is " a test', ["this", "is ", "a", "test"]),
        ("this 'is ' a test", ["this", "is ", "a", "test"]),
    ]
    bad_examples = [
        ('this "is a" test"', ValueError),
        ("this 'is a' test'", ValueError),
        ('this ""is " a test', ValueError),
    ]

    def test_good_examples(self):
        for i, example in enumerate(self.good_examples):
            text, expected = example
            try:
                self.assertEqual(expected, split(text))
            except AssertionError as e:
                raise AssertionError(i + 1, text) from e
            except ValueError as e:
                raise AssertionError(i + 1, text) from e

    def test_bad_examples(self):
        for text, expected in self.bad_examples:
            with self.assertRaises(expected):
                split(text)