from unittest import TestCase

from kvstore.application import KVStore, split
from kvstore.domainmodel import KVAggregate
from kvstore.exceptions import AggregateVersionMismatch


class TestHashCommands(TestCase):
    def setUp(self) -> None:
        self.app = KVStore()

    def test_hget_and_hset_commands(self):
        # Check we can get the field.
        self.assertEqual(self.app.execute_query("HGET myhash field"), None)

        # Create and execute proposal to set field.
        proposal = self.app.create_proposal('HSET myhash field "value"')
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check we can get the field.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Check we can't execute same proposal twice.
        with self.assertRaises(AggregateVersionMismatch):
            self.app.execute_proposal(proposal)

        # Create and execute proposal to update field.
        proposal = self.app.create_proposal("HSET myhash field value2")
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check we can get the updated field value.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value2")

        # Check we can't execute same proposal twice.
        with self.assertRaises(AggregateVersionMismatch):
            self.app.execute_proposal(proposal)

        # Check we can get the field.
        self.assertEqual(self.app.execute_query("HGET myhash2 field"), None)

        # Create and execute proposal to set field.
        proposal = self.app.create_proposal('HSET myhash2 field "value"')
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check we can get the field.
        self.assertEqual(self.app.execute_query("HGET myhash2 field"), "value")

        # Check we can't execute same proposal twice.
        with self.assertRaises(AggregateVersionMismatch):
            self.app.execute_proposal(proposal)

    def test_hdel_command(self):
        # Create and execute proposal to set field.
        proposal = self.app.create_proposal('HSET myhash field "value"')
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check the field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Propose and execute command to del field.
        proposal = self.app.create_proposal("HDEL myhash field")
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check the field has been deleted.
        self.assertEqual(self.app.execute_query("HGET myhash field"), None)

    def test_hsetnx_command(self):
        # Create and execute proposal to set field.
        proposal = self.app.create_proposal('HSET myhash field "value"')
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check the field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Propose and execute command to update field if not exists.
        proposal = self.app.create_proposal("HSETNX myhash field value2")
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check existing field has not updated.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Propose and execute command to update field if not exists.
        proposal = self.app.create_proposal("HSETNX myhash field2 value2")
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check the new field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field2"), "value2")

    def test_hincrby_command(self):
        # Create and execute proposal to set field.
        proposal = self.app.create_proposal("HSET myhash field 2")
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check the field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "2")

        # Propose and execute command to increment field.
        proposal = self.app.create_proposal("HINCRBY myhash field 5")
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check the field has been incremented.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "7")

        # Propose and execute command to decrement field.
        proposal = self.app.create_proposal("HINCRBY myhash field -3")
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check existing field has been decremented.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "4")

        # Check we can't do it twice.
        with self.assertRaises(AggregateVersionMismatch):
            self.app.execute_proposal(proposal)

    def test_rename(self):
        # Create and execute proposal to set field.
        proposal = self.app.create_proposal('HSET myhash field "value"')
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check the field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Create and execute proposal to rename key.
        proposal = self.app.create_proposal("RENAME myhash yourhash")
        aggregates = self.app.execute_proposal(proposal)
        self.app.save(*aggregates)

        # Check the field has been renamed.
        self.assertEqual(self.app.execute_query("HGET myhash field"), None)
        self.assertEqual(self.app.execute_query("HGET yourhash field"), "value")


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
