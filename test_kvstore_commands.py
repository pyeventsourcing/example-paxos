from unittest import TestCase

from kvstore.application import KVStore, split
from kvstore.domainmodel import KVAggregate
from kvstore.exceptions import AggregateVersionMismatch


class TestHashCommands(TestCase):
    def setUp(self) -> None:
        self.app = KVStore()

    def test_hset_command(self):
        # Create and execute proposal to set field.
        proposal = self.app.create_proposal('HSET myhash field "value"')

        # Execute proposal.
        aggregate = self.app.execute_proposal(proposal)

        # Check field has been set.
        self.assertEqual(aggregate.get_field_value("field"), "value")

        # Save the hash aggregate.

        # Check we can't execute same proposal twice.
        self.app.save(aggregate)
        with self.assertRaises(AggregateVersionMismatch):
            self.app.execute_proposal(proposal)

        # Propose and execute command to update field.
        proposal = self.app.create_proposal('HSET myhash field "value2"')
        aggregate = self.app.execute_proposal(proposal)

        # Check the field has been updated.
        self.assertIsInstance(aggregate, KVAggregate)
        self.assertEqual(aggregate.get_field_value("field"), "value2")

        # Check we can't do it twice.
        self.app.save(aggregate)
        with self.assertRaises(AggregateVersionMismatch):
            self.app.execute_proposal(proposal)

    def test_hget_command(self):
        # Save a hash aggregate.
        aggregate = KVAggregate("myhash")
        aggregate.set_field_value("field", "value")
        self.app.save(aggregate)

        # Check we can get the field value.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

    def test_hdel_command(self):
        # Save a hash aggregate.
        aggregate = KVAggregate("myhash")
        aggregate.set_field_value("field", "value")
        self.app.save(aggregate)

        # Propose and execute command to del field.
        proposal = self.app.create_proposal("HDEL myhash field")
        aggregate = self.app.execute_proposal(proposal)

        # Check the field has been deleted.
        self.assertIsInstance(aggregate, KVAggregate)
        self.assertEqual(aggregate.get_field_value("field"), None)

    def test_hsetnx_command(self):
        # Save a hash aggregate.
        aggregate = KVAggregate("myhash")
        aggregate.set_field_value("field", "value")
        self.app.save(aggregate)

        # Propose and execute command to del field.
        proposal = self.app.create_proposal("HSETNX myhash field value2")
        aggregate = self.app.execute_proposal(proposal)

        # Check existing field has not updated.
        self.assertIsInstance(aggregate, KVAggregate)
        self.assertEqual(aggregate.get_field_value("field"), "value")

        # Propose and execute command to del field.
        proposal = self.app.create_proposal("HSETNX myhash field2 value2")
        aggregate = self.app.execute_proposal(proposal)

        # Check the new field has been set.
        self.assertIsInstance(aggregate, KVAggregate)
        self.assertEqual(aggregate.get_field_value("field2"), "value2")

    def test_hincrby_command(self):
        # Save a hash aggregate.
        aggregate = KVAggregate("myhash")
        aggregate.set_field_value("field", "2")
        self.app.save(aggregate)

        # Propose and execute command to increment field.
        proposal = self.app.create_proposal("HINCRBY myhash field 5")
        aggregate = self.app.execute_proposal(proposal)

        # Check existing field has been incremented.
        self.assertIsInstance(aggregate, KVAggregate)
        self.assertEqual(aggregate.get_field_value("field"), "7")

        # Propose and execute command to decrement field.
        self.app.save(aggregate)
        proposal = self.app.create_proposal("HINCRBY myhash field -3")
        aggregate = self.app.execute_proposal(proposal)

        # Check existing field has been decremented.
        self.assertIsInstance(aggregate, KVAggregate)
        self.assertEqual(aggregate.get_field_value("field"), "4")

        # Check we can't do it twice.
        self.app.save(aggregate)
        with self.assertRaises(AggregateVersionMismatch):
            self.app.execute_proposal(proposal)


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