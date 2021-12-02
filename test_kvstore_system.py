import sys
from queue import Empty, Queue
from threading import Thread
from time import sleep, time
from typing import Any, Dict, Type, cast
from unittest import TestCase

from eventsourcing.application import AggregateNotFound
from eventsourcing.persistence import InfrastructureFactory
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.system import MultiThreadedRunner, Runner, SingleThreadedRunner
from eventsourcing.tests.ramdisk import tmpfile_uris
from eventsourcing.utils import retry

from keyvaluestore.application import KeyValueStore
from replicatedstatemachine.application import CommandFuture, StateMachineReplica
from paxossystem.system import PaxosSystem
from test_paxos_system import drop_postgres_table


class KeyValueSystemTestCase(TestCase):
    persistence_module: str = "eventsourcing.popo"
    runner_class: Type[Runner] = SingleThreadedRunner
    num_participants = 3

    def setUp(self):
        self.system = PaxosSystem(KeyValueStore, self.num_participants)
        self.runner = self.create_runner(
            {
                StateMachineReplica.COMMAND_CLASS: "keyvaluestore.commands:KeyValueStoreCommand",
                InfrastructureFactory.PERSISTENCE_MODULE: self.persistence_module,
                StateMachineReplica.AGGREGATE_CACHE_MAXSIZE: 500,
                StateMachineReplica.AGGREGATE_CACHE_FASTFORWARD: "n",
                # "COMPRESSOR_TOPIC": "zlib"
            }
        )
        self.runner.start()

    def create_runner(self, env: Dict):
        return self.runner_class(system=self.system, env=env)

    def tearDown(self):
        self.runner.stop()

    def get_app(self, name) -> StateMachineReplica:
        return cast(StateMachineReplica, self.runner.apps.get(name))

    @retry(
        (AggregateNotFound, AssertionError), max_attempts=1000, wait=0.005, stall=0.005
    )
    def assert_query(
        self, app: StateMachineReplica, cmd_text: str, expected_value: Any
    ):
        self.assertEqual(app.execute_query(cmd_text), expected_value)

    def close_connections_before_forking(self):
        """Implemented by the DjangoTestCase class."""
        pass

    def has_runner_errored(self):
        if isinstance(self.runner, MultiThreadedRunner):
            return self.runner.has_errored.is_set()
        else:
            return False


class TestWithSQLite(KeyValueSystemTestCase):
    persistence_module = "eventsourcing.sqlite"

    def setUp(self):
        self.temp_files = tmpfile_uris()
        super().setUp()

    def create_runner(self, env: Dict):
        for i in range(self.num_participants):
            env[f"KEYVALUESTORE{i}_SQLITE_DBNAME"] = next(self.temp_files)
            # env[f"KEYVALUESTORE{i}_SQLITE_DBNAME"] = f"file:application{i}?mode=memory&cache=shared"
        return super().create_runner(env)


class TestWithPostgreSQL(KeyValueSystemTestCase):
    persistence_module = "eventsourcing.postgres"

    def create_runner(self, env: Dict):
        env["POSTGRES_DBNAME"] = "eventsourcing"
        env["POSTGRES_HOST"] = "127.0.0.1"
        env["POSTGRES_PORT"] = "5432"
        env["POSTGRES_USER"] = "eventsourcing"
        env["POSTGRES_PASSWORD"] = "eventsourcing"
        return super().create_runner(env)

    def setUp(self):
        datastore = PostgresDatastore(
            "eventsourcing",
            "127.0.0.1",
            "5432",
            "eventsourcing",
            "eventsourcing",
        )
        for i in range(self.num_participants):
            drop_postgres_table(datastore, f"keyvaluestore{i}_events")
            drop_postgres_table(datastore, f"keyvaluestore{i}_snapshots")
            drop_postgres_table(datastore, f"keyvaluestore{i}_tracking")
        super().setUp()


class TestSystemSingleThreaded(KeyValueSystemTestCase):
    def test_propose_command_execute_query(self):

        apps = [self.get_app(f"KeyValueStore{i}") for i in range(self.num_participants)]
        app0 = apps[0]

        # Check each process has expected initial value.
        for app in apps:
            self.assert_query(app, "HGET myhash field", None)

        # Set a value.
        f = app0.propose_command('HSET myhash field "Hello"', assume_leader=True)
        f.result()

        # Check each process has expected final value.
        for app in apps:
            self.assert_query(app, "HGET myhash field", "Hello")

        # Update the value.
        f = app0.propose_command('HSET myhash field "Helloooo"', assume_leader=True)
        f.result()

        # Check each process has expected final value.
        for app in apps:
            self.assert_query(app, "HGET myhash field", "Helloooo")

        # Delete the value.
        f = app0.propose_command("HDEL myhash field", assume_leader=True)
        f.result()

        for app in apps:
            self.assert_query(app, "HGET myhash field", None)

        # Set a value in a different hash.
        f = app0.propose_command('HSET myhash2 field "Goodbye"', assume_leader=True)
        f.result()

        for app in apps:
            self.assert_query(app, "HGET myhash2 field", "Goodbye")


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


class TestPerformanceSingleThreaded(KeyValueSystemTestCase):
    period = 100

    def test_performance(self):
        print(type(self))
        apps = [self.get_app(f"KeyValueStore{i}") for i in range(self.num_participants)]

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
            cmd_text = f'HSET myhash{i} field "Hello{i}"'
            app.propose_command(
                cmd_text=cmd_text,
                assume_leader=True,
                results_queue=results_queue,
            )

            future = results_queue.get(timeout=0.5)

            future.result()
            i_started: float = future.started
            i_finished: float = future.finished
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
                        f"{i/duration:.1f}/s, "
                        f"{duration/i:.3f}s/item, "
                        f"started {period_started_count/period_duration:.1f}/s, "
                        f"finished {period/finished_duration:.1f}/s, "
                        f"{finished_duration/period:.3f}s/item, "
                        f"lat {avg_latency:.3f}s"
                    )
                    last_period_started = period_started
                    last_started_count = started_count

            if self.has_runner_errored():
                return

        rate = n / (finished_times[n] - started_times[0])
        print()
        print(f"Rate: {rate:.1f}/s")
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
    period = 40


class TestPerformanceSingleThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestPerformanceSingleThreaded
):
    period = 25


class TestPerformanceMultiThreaded(KeyValueSystemTestCase):
    runner_class = MultiThreadedRunner
    target_rate = 200

    def test_performance(self):
        print(type(self))
        apps = [self.get_app(f"KeyValueStore{i}") for i in range(self.num_participants)]

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
                # app = apps[i % self.num_participants]
                # app = random.choice(apps)
                app = apps[0]
                now = time()
                started_times.append(now)
                app.propose_command(
                    f'HSET myhash field "Hello{i}"',
                    assume_leader=True,
                    results_queue=results_queue,
                )
                # now = time()
                # started_times.append(now)
                # app.propose_command(
                #     f'HSET myhash{i} field "Helloooo{i}"',
                #     assume_leader=True,
                #     results_queue=results_queue,
                # )
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
                        future.result()
                        i_finished: float = future.finished
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
                                    f"{i/duration:.1f}/s, "
                                    f"{duration/i:.3f}s/item, "
                                    f"started {period_started_count/period_duration:.1f}/s, "
                                    f"finished {period/finished_duration:.1f}/s, "
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
        print(f"Rate: {rate:.1f}/s")
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


class TestPerformanceMultiThreadedWithSQLite(
    TestWithSQLite, TestPerformanceMultiThreaded
):
    target_rate = 40


class TestPerformanceMultiThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestPerformanceMultiThreaded
):
    target_rate = 25


del KeyValueSystemTestCase
