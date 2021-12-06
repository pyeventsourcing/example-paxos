from decimal import Decimal
from unittest import TestCase

from keyvaluestore.application import KeyValueReplica
from keyvaluestore.commands import split


class AppTestCase(TestCase):
    app: KeyValueReplica

    def assert_execute(self, cmd_text, expected_value):
        self.assertEqual(self.app.propose_command(cmd_text).result(), expected_value)


class TestHashCommands(AppTestCase):
    def setUp(self) -> None:
        self.app = KeyValueReplica(
            env={
                "COMMAND_CLASS": "keyvaluestore.commands:KeyValueCommand",
            }
        )

    def test_hget_and_hset_commands(self):
        # Check we can get the field.
        cmd_text = "HGET myhash field"
        expected_value = None
        self.assert_execute(cmd_text, expected_value)

        # Create and execute proposal to set field.
        result = self.app.propose_command('HSET myhash field "value"').result()
        self.assertEqual(result, 1)

        # Check we can get the field.
        self.assert_execute("HGET myhash field", "value")

        # Create and execute proposal to update field.
        result = self.app.propose_command("HSET myhash field value2").result()
        self.assertEqual(result, 1)

        # Check we can get the updated field value.
        self.assert_execute("HGET myhash field", "value2")

        # Check we can get the field.
        self.assert_execute("HGET myhash2 field", None)

        # Create and execute proposal to set field.
        self.app.propose_command('HSET myhash2 field "value"')

        # Check we can get the field.
        self.assert_execute("HGET myhash2 field", "value")

    def test_hdel_command(self):
        # Create and execute proposal to set field.
        self.app.propose_command('HSET myhash field "value"')

        # Check the field has been set.
        self.assert_execute("HGET myhash field", "value")

        # Propose and execute command to del field.
        result = self.app.propose_command("HDEL myhash field").result()
        self.assertEqual(result, 1)

        # Check the field has been deleted.
        self.assert_execute("HGET myhash field", None)

        # Propose and execute command to del field.
        result = self.app.propose_command("HDEL myhash field").result()
        self.assertEqual(result, 0)

    def test_hsetnx_command(self):
        # Create and execute proposal to set field.
        result = self.app.propose_command('HSET myhash field "value"').result()
        self.assertEqual(result, 1)

        # Check the field has been set.
        self.assert_execute("HGET myhash field", "value")

        # Propose and execute command to update field if not exists.
        result = self.app.propose_command("HSETNX myhash field value2").result()
        self.assertEqual(result, 0)

        # Check existing field has not updated.
        self.assert_execute("HGET myhash field", "value")

        # Check non-existing field has not been set.
        self.assert_execute("HGET myhash field2", None)

        # Propose and execute command to update field if not exists.
        result = self.app.propose_command("HSETNX myhash field2 value2").result()
        self.assertEqual(result, 1)

        # Check the new field has been set.
        self.assert_execute("HGET myhash field2", "value2")

    def test_hincrby_command(self):
        # Create and execute proposal to set field.
        result = self.app.propose_command("HSET myhash field 2").result()
        self.assertEqual(result, 1)

        # Check the field has been set.
        self.assert_execute("HGET myhash field", "2")

        # Propose and execute command to increment field.
        result = self.app.propose_command("HINCRBY myhash field 5").result()
        self.assertEqual(result, Decimal("7"))

        # Check the field has been incremented.
        self.assert_execute("HGET myhash field", "7")

        # Propose and execute command to decrement field.
        result = self.app.propose_command("HINCRBY myhash field -3").result()
        self.assertEqual(result, Decimal("4"))

        # Check existing field has been decremented.
        self.assert_execute("HGET myhash field", "4")

    def test_rename(self):
        # Create and execute proposal to set field.
        self.app.propose_command('HSET myhash field "value"')

        # Check the field has been set.
        self.assert_execute("HGET myhash field", "value")

        # Create and execute proposal to rename key.
        result = self.app.propose_command("RENAME myhash yourhash").result()
        self.assertEqual(result, "OK")

        # Check the field has been renamed.
        self.assert_execute("HGET myhash field", None)
        self.assert_execute("HGET yourhash field", "value")


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
