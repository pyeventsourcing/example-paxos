from __future__ import annotations

import os
import sys
from queue import Empty, Queue
from threading import Thread
from time import sleep, time
from typing import Any, cast
from unittest import TestCase

from eventsourcing.application import AggregateNotFound
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.system import MultiThreadedRunner
from eventsourcing.tests.ramdisk import tmpfile_uris
from eventsourcing.utils import retry

from kvstore.application import (
    CommandFuture,
    KVStore,
    execute_proposal,
    propose_command,
    split,
)
from kvstore.exceptions import AggregateVersionMismatch
from kvstore.domainmodel import HashAggregate
from paxos.system import PaxosSystem
from test_paxos_system import drop_postgres_table


class TestKVSystem(TestCase):
    def setUp(self):
        # Use the same system object in all tests.

        # os.environ["POSTGRES_DBNAME"] = "eventsourcing"
        # os.environ["POSTGRES_HOST"] = "127.0.0.1"
        # os.environ["POSTGRES_PORT"] = "5432"
        # os.environ["POSTGRES_USER"] = "eventsourcing"
        # os.environ["POSTGRES_PASSWORD"] = "eventsourcing"
        #
        # db = PostgresDatastore(
        #     os.getenv("POSTGRES_DBNAME"),
        #     os.getenv("POSTGRES_HOST"),
        #     os.getenv("POSTGRES_PORT"),
        #     os.getenv("POSTGRES_USER"),
        #     os.getenv("POSTGRES_PASSWORD"),
        # )
        # drop_postgres_table(db, "kvstore0_events")
        # drop_postgres_table(db, "kvstore0_snapshots")
        # drop_postgres_table(db, "kvstore0_tracking")
        # drop_postgres_table(db, "kvstore1_events")
        # drop_postgres_table(db, "kvstore1_snapshots")
        # drop_postgres_table(db, "kvstore1_tracking")
        # drop_postgres_table(db, "kvstore2_events")
        # drop_postgres_table(db, "kvstore2_snapshots")
        # drop_postgres_table(db, "kvstore2_tracking")
        #
        # self.temp_files = tmpfile_uris()

        # os.environ['KVSTORE0_SQLITE_DBNAME'] = next(self.temp_files)
        # os.environ['KVSTORE1_SQLITE_DBNAME'] = next(self.temp_files)
        # os.environ['KVSTORE2_SQLITE_DBNAME'] = next(self.temp_files)

        # os.environ[
        #     "KVSTORE0_SQLITE_DBNAME"
        # ] = "file:application0?mode=memory&cache=shared"
        # os.environ[
        #     "KVSTORE1_SQLITE_DBNAME"
        # ] = "file:application1?mode=memory&cache=shared"
        # os.environ[
        #     "KVSTORE2_SQLITE_DBNAME"
        # ] = "file:application2?mode=memory&cache=shared"

        # os.environ['INFRASTRUCTURE_FACTORY'] = 'eventsourcing.sqlite:Factory'
        # os.environ['INFRASTRUCTURE_FACTORY'] = 'eventsourcing.postgres:Factory'
        # os.environ['COMPRESSOR_TOPIC'] = 'zlib'
        self.num_participants = 3
        self.system = PaxosSystem(KVStore, self.num_participants)
        # self.runner = SingleThreadedRunner(self.system)
        self.runner = MultiThreadedRunner(self.system)
        self.runner.start()

    def tearDown(self):
        self.runner.stop()

    def test_propose_command_execute_query(self):

        app0 = self.get_paxos_app("KVStore0")
        app1 = self.get_paxos_app("KVStore1")
        app2 = self.get_paxos_app("KVStore2")

        # Check each process has expected initial value.
        self.assert_query(app0, "HGET myhash field", None)
        self.assert_query(app1, "HGET myhash field", None)
        self.assert_query(app2, "HGET myhash field", None)

        assert isinstance(app0, KVStore)

        # Set a value.
        f = app0.propose_command('HSET myhash field "Hello"', assume_leader=True)
        f.result()

        # Check each process has expected final value.
        self.assert_query(app0, "HGET myhash field", "Hello")
        self.assert_query(app1, "HGET myhash field", "Hello")
        self.assert_query(app2, "HGET myhash field", "Hello")

        # Update the value.
        f = app0.propose_command('HSET myhash field "Helloooo"', assume_leader=True)
        f.result()

        # Check each process has expected final value.
        self.assert_query(app0, "HGET myhash field", "Helloooo")
        self.assert_query(app1, "HGET myhash field", "Helloooo")
        self.assert_query(app2, "HGET myhash field", "Helloooo")

        # Delete the value.
        f = app0.propose_command("HDEL myhash field", assume_leader=True)
        f.result()

        self.assert_query(app0, "HGET myhash field", None)
        self.assert_query(app1, "HGET myhash field", None)
        self.assert_query(app2, "HGET myhash field", None)

    def get_paxos_app(self, name) -> KVStore:
        return cast(KVStore, self.runner.apps.get(name))

    @retry(
        (AggregateNotFound, AssertionError), max_attempts=1000, wait=0.005, stall=0.005
    )
    def assert_query(self, app: KVStore, cmd_text: str, expected_value: Any):
        self.assertEqual(app.execute_query(cmd_text), expected_value)

    def close_connections_before_forking(self):
        """Implemented by the DjangoTestCase class."""
        pass

    def test_performance(self):

        apps = [self.get_paxos_app(f"KVStore{i}") for i in range(self.num_participants)]

        n = 25

        started_times = []
        finished_times = []
        timings = []
        latencies = []
        results_queue: "Queue[CommandFuture]" = Queue()

        def write():
            interval = 1 / 50
            started = time()
            for i in list(range(n + 1)):
                app = apps[i % self.num_participants]
                # app = random.choice(apps)
                # app = apps[0]
                # started_times.append(now)
                now = time()
                started_times.append(now)
                app.propose_command(
                    f'HSET myhash{i} field "Hello{i}"',
                    assume_leader=True,
                    results_queue=results_queue,
                )
                now = time()
                sleep_for = max(0, started + (interval * (i + 1)) - now)
                # print("Sleeping for", sleep_for)
                sleep(sleep_for)
                if self.runner.has_stopped.is_set():
                    return

        period = 5

        def read():

            # Check each process has a resolution.
            for i in range(n + 1):
                is_queue_empty = True
                while is_queue_empty:
                    try:
                        future = results_queue.get(timeout=0.5)
                    except Empty:
                        if self.runner.has_stopped.is_set():
                            return
                    else:
                        is_queue_empty = False

                        i_started: float = future.started
                        i_finished: float = future.result()
                        timings.append((i_started, i_finished))
                        finished_times.append(i_finished)
                        i_latency = i_finished - i_started
                        latencies.append(i_latency)
                        if i % period == 0:
                            period_started = time()
                            started_count = len(started_times)
                            if i < period:
                                last_period_started = period_started
                                last_started_count = started_count
                                continue
                            else:
                                period_started_count = (
                                    started_count - last_started_count
                                )
                                duration = finished_times[i] - started_times[0]
                                period_duration = period_started - last_period_started
                                started_duration = (
                                    started_times[i] - started_times[i - period]
                                )
                                finished_duration = (
                                    i_finished - finished_times[i - period]
                                )
                                avg_latency = sum(latencies[i - period : i]) / period
                                print(
                                    f"Completed {i} commands in {duration:.1f}s: "
                                    f"{i/duration:.0f}/s, "
                                    f"{duration/i:.3f}s/item, "
                                    # f"{period/period_duration:.2f}/s, "
                                    # f"{period_duration/period:.3f}s/item, "
                                    f"started {period_started_count/period_duration:.0f}/s, "
                                    f"finished {period/finished_duration:.0f}/s, "
                                    f"{finished_duration/period:.3f}s/item, "
                                    f"lat {avg_latency:.3f}s"
                                )
                                sys.stdout.flush()
                                last_period_started = period_started
                                last_started_count = started_count

                        if self.runner.has_stopped.is_set():
                            return

        thread_write = Thread(target=write, daemon=True)
        thread_read = Thread(target=read, daemon=True)
        thread_write.start()
        thread_read.start()
        thread_write.join()
        thread_read.join()

        if self.runner.has_stopped.is_set():
            return

        rate = n / (finished_times[n] - started_times[0])
        print()
        print(f"Rate: {rate:.2f}/s")
        warm_up_period = 0
        print(f"Min latency: {min(latencies[warm_up_period:n]):.3f}s")
        print(
            f"Avg latency: {sum(latencies[warm_up_period:n]) / (n - warm_up_period):.3f}s"
        )
        print(f"Max latency: {max(latencies[warm_up_period:n]):.3f}s")

        sys.stdout.flush()
        sleep(0.1)


class TestKVAggregates(TestCase):
    def test_hash(self):
        a = HashAggregate("myhash")
        self.assertEqual(a.key_name, "myhash")
        self.assertEqual(a.get_field_value("field"), None)
        a.set_field_value("field", "value")
        self.assertEqual(a.get_field_value("field"), "value")


class TestKVStore(TestCase):
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
