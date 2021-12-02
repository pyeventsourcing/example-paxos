from unittest import TestCase

from replicatedstatemachine.application import StateMachineReplica
from keyvaluestore.commands import split


class TestHashCommands(TestCase):
    def setUp(self) -> None:
        self.app = StateMachineReplica(
            env={
                StateMachineReplica.COMMAND_CLASS: "keyvaluestore.commands:KeyValueStoreCommand",
            }
        )

    def test_hget_and_hset_commands(self):
        # Check we can get the field.
        self.assertEqual(self.app.execute_query("HGET myhash field"), None)

        # Create and execute proposal to set field.
        aggregates = self.app.execute_proposal('HSET myhash field "value"')
        self.app.save(*aggregates)

        # Check we can get the field.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Create and execute proposal to update field.
        aggregates = self.app.execute_proposal("HSET myhash field value2")
        self.app.save(*aggregates)

        # Check we can get the updated field value.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value2")

        # Check we can get the field.
        self.assertEqual(self.app.execute_query("HGET myhash2 field"), None)

        # Create and execute proposal to set field.
        aggregates = self.app.execute_proposal('HSET myhash2 field "value"')
        self.app.save(*aggregates)

        # Check we can get the field.
        self.assertEqual(self.app.execute_query("HGET myhash2 field"), "value")

    def test_hdel_command(self):
        # Create and execute proposal to set field.
        aggregates = self.app.execute_proposal('HSET myhash field "value"')
        self.app.save(*aggregates)

        # Check the field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Propose and execute command to del field.
        aggregates = self.app.execute_proposal("HDEL myhash field")
        self.app.save(*aggregates)

        # Check the field has been deleted.
        self.assertEqual(self.app.execute_query("HGET myhash field"), None)

    def test_hsetnx_command(self):
        # Create and execute proposal to set field.
        aggregates = self.app.execute_proposal('HSET myhash field "value"')
        self.app.save(*aggregates)

        # Check the field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Propose and execute command to update field if not exists.
        aggregates = self.app.execute_proposal("HSETNX myhash field value2")
        self.app.save(*aggregates)

        # Check existing field has not updated.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Propose and execute command to update field if not exists.
        aggregates = self.app.execute_proposal("HSETNX myhash field2 value2")
        self.app.save(*aggregates)

        # Check the new field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field2"), "value2")

    def test_hincrby_command(self):
        # Create and execute proposal to set field.
        aggregates = self.app.execute_proposal("HSET myhash field 2")
        self.app.save(*aggregates)

        # Check the field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "2")

        # Propose and execute command to increment field.
        aggregates = self.app.execute_proposal("HINCRBY myhash field 5")
        self.app.save(*aggregates)

        # Check the field has been incremented.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "7")

        # Propose and execute command to decrement field.
        aggregates = self.app.execute_proposal("HINCRBY myhash field -3")
        self.app.save(*aggregates)

        # Check existing field has been decremented.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "4")

    def test_rename(self):
        # Create and execute proposal to set field.
        aggregates = self.app.execute_proposal('HSET myhash field "value"')
        self.app.save(*aggregates)

        # Check the field has been set.
        self.assertEqual(self.app.execute_query("HGET myhash field"), "value")

        # Create and execute proposal to rename key.
        aggregates = self.app.execute_proposal("RENAME myhash yourhash")
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
