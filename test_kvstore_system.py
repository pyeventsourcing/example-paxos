import os
import random
import sys
from queue import Empty, Queue
from threading import Thread
from time import sleep, time
from typing import Any, Type, cast
from unittest import TestCase

from eventsourcing.application import AggregateNotFound
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.system import MultiThreadedRunner, Runner, SingleThreadedRunner
from eventsourcing.tests.ramdisk import tmpfile_uris
from eventsourcing.utils import retry

from kvstore.application import CommandFuture
from kvstore.application import KVStore
from paxos.system import PaxosSystem
from test_paxos_system import drop_postgres_table


class TestKVSystem(TestCase):
    persistence_module: str = "eventsourcing.popo"
    runner_class: Type[Runner] = SingleThreadedRunner

    def setUp(self):
        # Use the same system object in all tests.

        os.environ["PERSISTENCE_MODULE"] = self.persistence_module
        # os.environ['COMPRESSOR_TOPIC'] = 'zlib'
        self.num_participants = 3
        self.system = PaxosSystem(KVStore, self.num_participants)
        self.runner = self.runner_class(self.system)
        self.runner.start()

    def tearDown(self):
        self.runner.stop()
        del os.environ["PERSISTENCE_MODULE"]

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

    def has_runner_errored(self):
        if isinstance(self.runner, MultiThreadedRunner):
            return self.runner.has_errored.is_set()
        else:
            return False


class TestWithSQLite(TestKVSystem):
    persistence_module = "eventsourcing.sqlite"

    def setUp(self):
        self.temp_files = tmpfile_uris()

        os.environ["KVSTORE0_SQLITE_DBNAME"] = next(self.temp_files)
        os.environ["KVSTORE1_SQLITE_DBNAME"] = next(self.temp_files)
        os.environ["KVSTORE2_SQLITE_DBNAME"] = next(self.temp_files)

        # os.environ[
        #     "KVSTORE0_SQLITE_DBNAME"
        # ] = "file:application0?mode=memory&cache=shared"
        # os.environ[
        #     "KVSTORE1_SQLITE_DBNAME"
        # ] = "file:application1?mode=memory&cache=shared"
        # os.environ[
        #     "KVSTORE2_SQLITE_DBNAME"
        # ] = "file:application2?mode=memory&cache=shared"
        super().setUp()


class TestWithPostgreSQL(TestKVSystem):
    persistence_module = "eventsourcing.postgres"

    def setUp(self):
        os.environ["POSTGRES_DBNAME"] = "eventsourcing"
        os.environ["POSTGRES_HOST"] = "127.0.0.1"
        os.environ["POSTGRES_PORT"] = "5432"
        os.environ["POSTGRES_USER"] = "eventsourcing"
        os.environ["POSTGRES_PASSWORD"] = "eventsourcing"

        db = PostgresDatastore(
            os.getenv("POSTGRES_DBNAME"),
            os.getenv("POSTGRES_HOST"),
            os.getenv("POSTGRES_PORT"),
            os.getenv("POSTGRES_USER"),
            os.getenv("POSTGRES_PASSWORD"),
        )
        drop_postgres_table(db, "kvstore0_events")
        drop_postgres_table(db, "kvstore0_snapshots")
        drop_postgres_table(db, "kvstore0_tracking")
        drop_postgres_table(db, "kvstore1_events")
        drop_postgres_table(db, "kvstore1_snapshots")
        drop_postgres_table(db, "kvstore1_tracking")
        drop_postgres_table(db, "kvstore2_events")
        drop_postgres_table(db, "kvstore2_snapshots")
        drop_postgres_table(db, "kvstore2_tracking")

        super().setUp()


class TestSystemSingleThreaded(TestKVSystem):
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

        # Set a value in a different hash.
        f = app0.propose_command('HSET myhash2 field "Hello"', assume_leader=True)
        f.result()


class TestSystemSingleThreadedWithSQLite(TestWithSQLite, TestSystemSingleThreaded):
    pass


class TestSystemSingleThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestSystemSingleThreaded
):
    pass


class TestSystemMultiThreaded(TestSystemSingleThreaded):
    runner_class = MultiThreadedRunner


class TestSystemMultiThreadedWithSQLite(TestWithSQLite, TestSystemMultiThreaded):
    pass


class TestSystemMultiThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestSystemMultiThreaded
):
    pass


class TestPerformanceSingleThreaded(TestKVSystem):
    period = 100

    def test_performance(self):
        print(type(self))
        apps = [self.get_paxos_app(f"KVStore{i}") for i in range(self.num_participants)]

        period = self.period
        n = period * 10

        started_times = []
        finished_times = []
        timings = []
        latencies = []
        results_queue: "Queue[CommandFuture]" = Queue()

        for i in list(range(n + 1)):
            # app = apps[i % self.num_participants]
            # app = random.choice(apps)
            app = apps[0]
            now = time()
            started_times.append(now)
            app.propose_command(
                f'HSET myhash{i} field "Hello{i}"',
                assume_leader=True,
                results_queue=results_queue,
            )

            future = results_queue.get(timeout=0.5)

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
                    period_started_count = started_count - last_started_count
                    duration = finished_times[i] - started_times[0]
                    period_duration = period_started - last_period_started
                    finished_duration = i_finished - finished_times[i - period]
                    avg_latency = sum(latencies[i - period : i]) / period
                    print(
                        f"Completed {i} commands in {duration:.1f}s: "
                        f"{i/duration:.0f}/s, "
                        f"{duration/i:.3f}s/item, "
                        f"started {period_started_count/period_duration:.0f}/s, "
                        f"finished {period/finished_duration:.0f}/s, "
                        f"{finished_duration/period:.3f}s/item, "
                        f"lat {avg_latency:.3f}s"
                    )
                    last_period_started = period_started
                    last_started_count = started_count

            if self.has_runner_errored():
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

        previous_finished_time = None
        for i, timing in enumerate(timings):
            started_time, finished_time = timing
            if previous_finished_time is None:
                continue
            if previous_finished_time > finished_time:
                print("Finished earlier:", i, started_time, finished_time)
            previous_finished_time = finished_time

        sys.stdout.flush()
        sleep(0.1)


class TestPerformanceSingleThreadedWithSQLite(
    TestWithSQLite, TestPerformanceSingleThreaded
):
    period = 50


class TestPerformanceSingleThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestPerformanceSingleThreaded
):
    period = 15


class TestPerformanceMultiThreaded(TestKVSystem):
    runner_class = MultiThreadedRunner
    target_rate = 50

    def test_performance(self):
        print(type(self))
        apps = [self.get_paxos_app(f"KVStore{i}") for i in range(self.num_participants)]

        period = self.target_rate
        interval = 1 / period
        n = period * 10

        started_times = []
        finished_times = []
        timings = []
        latencies = []
        results_queue: "Queue[CommandFuture]" = Queue()

        def write():
            started = time()
            for i in list(range(n + 1)):
                app = apps[i % self.num_participants]
                app = random.choice(apps)
                # app = apps[0]
                now = time()
                started_times.append(now)
                app.propose_command(
                    f'HSET myhash{i} field "Hello{i}"',
                    assume_leader=True,
                    results_queue=results_queue,
                )
                now = time()
                sleep_for = max(0, started + (interval * (i + 1)) - now)
                sleep(sleep_for)
                if self.has_runner_errored():
                    return

        def read():

            # Check each process has a resolution.
            for i in range(n + 1):
                is_queue_empty = True
                while is_queue_empty:
                    try:
                        future = results_queue.get(timeout=0.5)
                    except Empty:
                        if self.has_runner_errored():
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
                                finished_duration = (
                                    i_finished - finished_times[i - period]
                                )
                                avg_latency = sum(latencies[i - period : i]) / period
                                print(
                                    f"Completed {i} commands in {duration:.1f}s: "
                                    f"{i/duration:.0f}/s, "
                                    f"{duration/i:.3f}s/item, "
                                    f"started {period_started_count/period_duration:.0f}/s, "
                                    f"finished {period/finished_duration:.0f}/s, "
                                    f"{finished_duration/period:.3f}s/item, "
                                    f"lat {avg_latency:.3f}s"
                                )
                                last_period_started = period_started
                                last_started_count = started_count

                        if self.has_runner_errored():
                            return

        thread_write = Thread(target=write, daemon=True)
        thread_write.start()
        read()

        if self.has_runner_errored():
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

        previous_finished_time = None
        for i, timing in enumerate(timings):
            started_time, finished_time = timing
            if previous_finished_time is None:
                continue
            if previous_finished_time > finished_time:
                print("Finished earlier:", i, started_time, finished_time)
            previous_finished_time = finished_time

        sleep(5)
        for app in apps:
            print(
                "Max notification ID:",
                app.__class__.__name__,
                app.recorder.max_notification_id(),
            )

        sys.stdout.flush()
        sleep(0.1)


class TestPerformanceMultiThreadedWithSQLite(
    TestWithSQLite, TestPerformanceMultiThreaded
):
    target_rate = 50


class TestPerformanceMultiThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestPerformanceMultiThreaded
):
    target_rate = 35


del TestKVSystem
