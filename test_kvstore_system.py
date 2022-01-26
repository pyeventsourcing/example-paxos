import gc
import resource
import sys
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from itertools import count
from queue import Queue
from threading import Event, Thread
from time import sleep, time
from typing import Dict, Type, cast, Optional
from unittest import TestCase

import eventsourcing.utils
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.system import MultiThreadedRunner, Runner, SingleThreadedRunner
from eventsourcing.tests.persistence import tmpfile_uris
from eventsourcing.tests.postgres_utils import drop_postgres_table
from eventsourcing.utils import retry

from keyvaluestore.application import KeyValueReplica
from paxossystem.domainmodel import PaxosAggregate
from replicatedstatemachine.application import (
    StateMachineReplica,
    create_command_proposal_id_from_round,
)
from replicatedstatemachine.commandfutures import CommandFuture
from paxossystem.system import PaxosSystem
from replicatedstatemachine.exceptions import CommandError, CommandTimeoutError


class KeyValueSystemTestCase(TestCase):
    persistence_module: str = "eventsourcing.popo"
    runner_class: Type[Runner] = SingleThreadedRunner
    num_participants = 3
    pool_size = 5
    max_overflow = 0
    aggregate_cache_max_size = 500

    def setUp(self):
        eventsourcing.utils._topic_cache.clear()
        self.system = PaxosSystem(KeyValueReplica, self.num_participants)
        self.runner = self.create_runner(
            {
                "PERSISTENCE_MODULE": self.persistence_module,
                "AGGREGATE_CACHE_MAXSIZE": self.aggregate_cache_max_size,
                "AGGREGATE_CACHE_FASTFORWARD": "n",
                # "COMPRESSOR_TOPIC": "zlib"
            }
        )
        self.runner.start()

    def create_runner(self, env: Dict):
        return self.runner_class(system=self.system, env=env)

    def tearDown(self):
        self.runner.stop()
        eventsourcing.utils._topic_cache.clear()
        super().tearDown()

    def get_app(self, name) -> StateMachineReplica:
        return cast(StateMachineReplica, self.runner.apps.get(name))

    def close_connections_before_forking(self):
        """Implemented by the DjangoTestCase class."""
        pass

    def has_runner_errored(self):
        if isinstance(self.runner, MultiThreadedRunner):
            return self.runner.has_errored.is_set()
        else:
            return False

    def loop_malloc(self, stopped_event: Event):
        tracemalloc.start()
        while not stopped_event.is_set():
            self.print_malloc()
            sleep(1)

    def print_malloc(self):

        snapshot = tracemalloc.take_snapshot()

        # top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        top_stats = snapshot.statistics('lineno')
        maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print(f"[ Top 10 ] Max RSS", maxrss)
        count = 0
        for stat in top_stats:
            count += 1
            stat_str = str(stat)
            # if "decoder" not in stat_str:
            #     continue

            # if "eventsourcing" not in stat_str:
            #     break
            # else:
            #     stat_str = stat_str.replace(
            #         "/Users/john/PyCharmProjects/eventsourcing/", ""
            #     )
            print(stat_str)
            if count > 10:
                break

    def start_tracemalloc_thread(self):
        self.tracemalloc_thread_stopped = Event()
        self.tracemalloc_thread = Thread(target=self.loop_malloc, args=(self.tracemalloc_thread_stopped, ), daemon=True)
        self.tracemalloc_thread.start()


class TestWithSQLite(KeyValueSystemTestCase):
    persistence_module = "eventsourcing.sqlite"

    def setUp(self):
        self.temp_files = tmpfile_uris()
        super().setUp()

    def create_runner(self, env: Dict):
        app_name = KeyValueReplica.name.upper()
        for i in range(self.num_participants):
            env[f"{app_name}{i}_SQLITE_DBNAME"] = next(self.temp_files)
            # env[f"{app_name}{i}_SQLITE_DBNAME"] = f"file:application{i}?mode=memory&cache=shared"
        return super().create_runner(env)


class TestWithPostgreSQL(KeyValueSystemTestCase):
    persistence_module = "eventsourcing.postgres"

    def create_runner(self, env: Dict):
        env["POSTGRES_DBNAME"] = "eventsourcing"
        env["POSTGRES_HOST"] = "127.0.0.1"
        env["POSTGRES_PORT"] = "5432"
        env["POSTGRES_USER"] = "eventsourcing"
        env["POSTGRES_PASSWORD"] = "eventsourcing"
        env["POSTGRES_POOL_SIZE"] = str(self.pool_size)
        env["POSTGRES_POOL_MAX_OVERFLOW"] = str(self.max_overflow)
        env["POSTGRES_POOL_MAX_AGE"] = "30"
        return super().create_runner(env)

    def setUp(self):
        datastore = PostgresDatastore(
            "eventsourcing",
            "127.0.0.1",
            "5432",
            "eventsourcing",
            "eventsourcing",
        )
        app_name = KeyValueReplica.name.lower()
        for i in range(self.num_participants):
            drop_postgres_table(datastore, f"{app_name}{i}_events")
            drop_postgres_table(datastore, f"{app_name}{i}_snapshots")
            drop_postgres_table(datastore, f"{app_name}{i}_tracking")
        datastore.close()
        super().setUp()


class TestSystemSingleThreaded(KeyValueSystemTestCase):
    def assert_result(self, app, cmd_text, expected_result):
        future = app.propose_command(cmd_text)
        result = future.result(timeout=1)
        self.assertEqual(result, expected_result)

    def test_propose_commands_no_leader(self):

        apps = [
            self.get_app(f"{KeyValueReplica.__name__}{i}")
            for i in range(self.num_participants)
        ]
        app0 = apps[0]

        # Check each process has expected initial value.
        for app in apps:
            self.assert_result(app, "HGET myhash field", None)

        # Set a value.
        app0.propose_command('HSET myhash field "Hello"').result()

        # Check each process has expected final value.
        for app in apps:
            self.assert_result(app, "HGET myhash field", "Hello")

        # Update the value.
        app0.propose_command('HSET myhash field "Helloooo"').result()

        # Check each process has expected final value.
        for app in apps:
            self.assert_result(app, "HGET myhash field", "Helloooo")

        # Delete the value.
        app0.propose_command("HDEL myhash field").result()

        for app in apps:
            self.assert_result(app, "HGET myhash field", None)

        # Set a value in a different hash.
        app0.propose_command('HSET myhash2 field "Goodbye"').result()

        for app in apps:
            self.assert_result(app, "HGET myhash2 field", "Goodbye")

    def test_forwarded_command(self):
        apps = [
            self.get_app(f"{KeyValueReplica.__name__}{i}")
            for i in range(self.num_participants)
        ]
        apps[0].elected_leader_uid = apps[0].name
        apps[1].elected_leader_uid = apps[0].name
        apps[2].elected_leader_uid = apps[0].name

        apps[1].propose_command(f'HSET myhash field "Hello World"').result(timeout=1)

    def test_propose_leader(self):

        apps = [
            self.get_app(f"{KeyValueReplica.__name__}{i}")
            for i in range(self.num_participants)
        ]

        # Check each process has expected initial value.
        for app in apps:
            self.assertIsNone(app.elected_leader_uid)

        apps[0].propose_leader()

        for app in apps:
            self.assert_elected_leader_uid(app, apps[0].name)

    @retry(AssertionError, max_attempts=10, wait=0.1)
    def assert_elected_leader_uid(self, app: StateMachineReplica, expected_value):
        if app.elected_leader_uid != expected_value:
            raise AssertionError(app.elected_leader_uid)

    @retry(AssertionError, max_attempts=10, wait=0.1)
    def assert_leader_is_elected(self, app: StateMachineReplica):
        if not app.elected_leader_uid:
            raise AssertionError(app.elected_leader_uid)
        print(app.name, "thinks", app.elected_leader_uid, "is leader")

    def test_leader_lease(self, run_for_duration=5, num_clients=4):
        apps = [
            self.get_app(f"{KeyValueReplica.__name__}{i}")
            for i in range(self.num_participants)
        ]

        # Configure leadership election mechanism.
        for app in apps:
            app.probability_leader_fails_to_propose_reelection = 0
            app.leader_lease = 10

        # Propose leader.
        propose_leader_i = 0
        apps[propose_leader_i].propose_leader()

        # Wait for leadership election to complete.
        for app in apps:
            self.assert_leader_is_elected(app)
            # self.assert_elected_leader_uid(app, apps[propose_leader_i].name)

        num_clients = len(apps) * 1

        started = time()
        last_period_started = started
        error_count = 0
        result_count = 0

        # sleep(run_for_duration)
        # return

        write_error = Event()
        finished = Event()

        counter = count()

        def write(app_i):
            nonlocal result_count, error_count, last_period_started
            while not write_error.is_set() and not finished.is_set():
                i = next(counter)

                # app = random.choice(apps)
                app = apps[app_i]

                try:
                    future = app.propose_command(f'HSET myhash{i} field "Hello World"')
                    future.result(timeout=5)
                except CommandTimeoutError:
                    error_count += 1
                    print(
                        f"Timed out waiting for command result {i}.",
                        "Error count:",
                        error_count,
                    )
                    if error_count > 50:
                        write_error.set()
                        finished.set()
                except CommandError as e:
                    error_count += 1
                    print(
                        f"Command error for {i}.",
                        "Error count:",
                        error_count,
                        e
                    )
                    write_error.set()
                    finished.set()
                    raise e
                else:
                    result_count += 1
                    period = 50
                    if result_count % period == 0:

                        last_period_ended = time()
                        print(
                            f"Got result from app {app.name} in",
                            f"{future.duration:.6f}",
                            "Results:",
                            result_count,
                            "Errors:",
                            error_count,
                            "Rate:",
                            f"{period / (last_period_ended - last_period_started):.2f}",
                        )
                        last_period_started = last_period_ended

                # End run after specified duration.
                if isinstance(self.runner, MultiThreadedRunner) and self.runner.has_errored.is_set():
                    finished.set()

                if time() - started > run_for_duration:
                    finished.set()

            if finished.is_set():
                print("Thread exiting, finished set!")
            if write_error.is_set():
                print("Thread existing, write error set!")

        thread_pool = ThreadPoolExecutor(max_workers=num_clients)
        for i in range(num_clients):
            thread_pool.submit(write, app_i=(i % len(apps)))

        try:
            finished.wait(run_for_duration)
        finally:
            finished.set()

        thread_pool.shutdown(cancel_futures=True)

        self.assertFalse(write_error.is_set())


class TestSystemSingleThreadedWithSQLite(TestWithSQLite, TestSystemSingleThreaded):
    def test_leader_lease(self):
        super().test_leader_lease()


class TestSystemSingleThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestSystemSingleThreaded
):
    def test_leader_lease(self):
        super().test_leader_lease()


class TestSystemMultiThreaded(TestSystemSingleThreaded):
    runner_class = MultiThreadedRunner

    def test_leader_lease(self):
        assert isinstance(self.runner, MultiThreadedRunner)
        super().test_leader_lease()


class TestSystemMultiThreadedWithSQLite(TestWithSQLite, TestSystemMultiThreaded):

    def test_leader_lease(self):
        super().test_leader_lease()


class TestSystemMultiThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestSystemMultiThreaded
):
    def test_propose_leader(self):
        super().test_propose_leader()

    def test_leader_lease(self):
        super().test_leader_lease()


class TestPerformanceSingleThreaded(KeyValueSystemTestCase):
    period = 100

    def test_performance(self):
        print(type(self))
        apps = [
            self.get_app(f"{KeyValueReplica.name}{i}")
            for i in range(self.num_participants)
        ]

        period = self.period
        n = period * 3

        started_times = []
        finished_times = []
        timings = []
        latencies = []

        for i in list(range(n + 1)):
            # app = apps[i % self.num_participants]
            # app = random.choice(apps)
            app = apps[0]
            app.assume_leader = True
            now = time()
            started_times.append(now)
            cmd_text = f'HSET myhash{i} field "Hello{i}"'
            future = app.propose_command(cmd_text)
            try:
                future.result(timeout=1)
            except CommandTimeoutError:
                raise Exception(
                    f"Command future timed out for '{future.original_cmd_text}'"
                )
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
        print(f"Min latency: {min(latencies[warm_up_period:n]):.4f}s")
        print(
            f"Avg latency: {sum(latencies[warm_up_period:n]) / (n - warm_up_period):.4f}s"
        )
        print(f"Max latency: {max(latencies[warm_up_period:n]):.4f}s")

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
    target_rate = 100

    def test_performance(self):
        eventsourcing.utils._topic_cache.clear()
        print(type(self))
        apps = [
            self.get_app(f"{KeyValueReplica.__name__}{i}")
            for i in range(self.num_participants)
        ]

        period = self.target_rate
        interval = 0.999 / period
        n = period * 3

        started_times = []
        finished_times = []
        timings = []
        latencies = []
        futures_queue: "Queue[CommandFuture]" = Queue()

        def write():
            started = time()

            apps[0].elected_leader_uid = apps[0].name
            apps[1].elected_leader_uid = apps[0].name
            apps[2].elected_leader_uid = apps[0].name

            for i in list(range(n + 1)):
                # app = apps[i % self.num_participants]
                # app = random.choice(apps)
                app = apps[0]

                now = time()
                started_times.append(now)
                future = app.propose_command(
                    f'HSET myhash{i} field "Hello{i} {app.name}"',
                )
                futures_queue.put(future)
                now = time()
                sleep_for = max(0, started + (interval * (i + 1)) - now)
                sleep(sleep_for)
                if self.has_runner_errored():
                    return

        def read():

            # Check each process has a resolution.
            for i in range(n + 1):
                future = futures_queue.get(timeout=1)
                i_started: float = future.started
                try:
                    future.result(timeout=1)
                except CommandError as e:
                    print("Command error:", e, future.original_cmd_text)
                i_finished = future.finished
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

        thread_write = Thread(target=write, daemon=True)
        thread_write.start()
        read()

        if self.has_runner_errored():
            return

        rate = n / (finished_times[n] - started_times[0])
        print()
        print(f"Rate: {rate:.1f}/s")
        warm_up_period = 0
        print(f"Min latency: {min(latencies[warm_up_period:n]):.4f}s")
        print(
            f"Avg latency: {sum(latencies[warm_up_period:n]) / (n - warm_up_period):.4f}s"
        )
        print(f"Max latency: {max(latencies[warm_up_period:n]):.4f}s")

        previous_finished_time = None
        for i, timing in enumerate(timings):
            started_time, finished_time = timing
            if previous_finished_time is None:
                continue
            if previous_finished_time > finished_time:
                print("Finished earlier:", i, started_time, finished_time)
            previous_finished_time = finished_time

        sys.stdout.flush()
        sleep(1)

        print("Checking paxos logs...")
        has_errors = False
        log_counts = []
        first_log_count = None
        for app in apps:
            log_count = len(list(app.command_log.get()))
            print(f"Application {app.name} has {log_count} items in log")
            log_counts.append(log_count)
            if first_log_count is None:
                first_log_count = log_count
            else:
                self.assertEqual(first_log_count, log_count, app.name)

        for paxos_logged in apps[0].command_log.get():
            aggregate_id = create_command_proposal_id_from_round(
                paxos_logged.originator_version
            )
            paxos_aggregate0 = cast(
                PaxosAggregate, apps[0].repository.get(aggregate_id)
            )
            paxos_aggregate1 = cast(
                PaxosAggregate, apps[1].repository.get(aggregate_id)
            )
            paxos_aggregate2 = cast(
                PaxosAggregate, apps[2].repository.get(aggregate_id)
            )
            if paxos_aggregate1.final_value != paxos_aggregate0.final_value:
                print(
                    "Log different in app 1 from app0 at position:",
                    paxos_aggregate0.final_value,
                )
                has_errors = True
            if paxos_aggregate1.final_value != paxos_aggregate0.final_value:
                print(
                    "Log different in app 1 from app0 at position:",
                    paxos_logged.originator_version,
                    paxos_aggregate1.final_value,
                    paxos_aggregate0.final_value,
                )
                has_errors = True
            if paxos_aggregate2.final_value != paxos_aggregate0.final_value:
                print(
                    "Log different in app 2 from app0 at position:",
                    paxos_logged.originator_version,
                    paxos_aggregate2.final_value,
                    paxos_aggregate0.final_value,
                )
                has_errors = True
        if not has_errors:
            print("All apps have same command logs")

        field_value0 = apps[0].propose_command("HGET myhash0 field").result()
        field_value1 = apps[0].propose_command("HGET myhash0 field").result()
        field_value2 = apps[0].propose_command("HGET myhash0 field").result()
        if field_value0 != field_value1:
            print("Field value different", field_value0, field_value1)
            has_errors = True
        if field_value0 != field_value2:
            print("Field value different", field_value0, field_value2)
            has_errors = True
        self.assertFalse(has_errors)
        print("Field value:", field_value0)


class TestPerformanceMultiThreadedWithSQLite(
    TestWithSQLite, TestPerformanceMultiThreaded
):
    target_rate = 100

    def test_performance(self):
        super().test_performance()


class TestPerformanceMultiThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestPerformanceMultiThreaded
):
    target_rate = 60


class TestMalloc(KeyValueSystemTestCase):

    persistence_module: str = "eventsourcing.popo"
    runner_class: Type[Runner] = MultiThreadedRunner

    num_participants = 3
    pool_size = 1
    max_overflow = 0
    aggregate_cache_max_size = 100

    def _test_malloc(self):
        tracemalloc.start()

        apps = [
            self.get_app(f"{KeyValueReplica.__name__}{i}")
            for i in range(self.num_participants)
        ]

        apps[0].propose_leader()


        print_period: Optional[int] = None

        def do_run(purpose, run_length):
            for i in range(run_length):

                if print_period is not None:
                    if not (i + 1) % print_period:
                        print(purpose, i + 1)

                # app0 = random.choice(apps)
                app0 = apps[0]

                future = app0.propose_command(f'HSET myhash{i} field "Hello World"')
                future.result(timeout=15)

            # for app in apps:
            #     print("Aggregate cache", app.name, len(app.repository.cache.cache))
            #     print("Connection pool", app.name, len(app.recorder.datastore.pool._pool))

        # do_run("Warming up", run_length=50)

        snapshot1 = tracemalloc.take_snapshot()

        run_count = 0
        while True:

            do_run("Running", run_length=10)
            run_count += 1
            gc.collect()
            snapshot2 = tracemalloc.take_snapshot()

            # top_stats = snapshot2.compare_to(snapshot1, 'lineno')
            top_stats = snapshot2.statistics('lineno')

            print(f"[ Top 10 at {run_count}]")
            count = 0
            for stat in top_stats:
                stat_str = str(stat)
                # if "decoder" not in stat_str:
                #     continue

                # if "eventsourcing" not in stat_str:
                #     break
                # else:
                #     stat_str = stat_str.replace(
                #         "/Users/john/PyCharmProjects/eventsourcing/", ""
                #     )
                print(stat_str)
                count += 1
                if count > 10:
                    break

            # snapshot1 = snapshot2


class TestMallocWithSQLite(TestWithSQLite, TestMalloc):
    pass


class TestMallocWithPosgreSQL(TestWithPostgreSQL, TestMalloc):
    pass


del KeyValueSystemTestCase
