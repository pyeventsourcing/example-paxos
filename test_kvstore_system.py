import random
import sys
from concurrent.futures import TimeoutError
from itertools import count
from queue import Queue
from threading import Thread
from time import sleep, time
from typing import Dict, Type, cast
from unittest import TestCase

import eventsourcing.utils
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.system import MultiThreadedRunner, Runner, SingleThreadedRunner
from eventsourcing.tests.ramdisk import tmpfile_uris
from eventsourcing.utils import retry

from keyvaluestore.application import KeyValueReplica
from paxossystem.domainmodel import PaxosAggregate
from replicatedstatemachine.application import (
    CommandFuture,
    StateMachineReplica,
    create_command_proposal_id_from_round,
)
from paxossystem.system import PaxosSystem
from replicatedstatemachine.exceptions import CommandFutureEvicted, CommandRejected
from test_paxos_system import drop_postgres_table


class KeyValueSystemTestCase(TestCase):
    persistence_module: str = "eventsourcing.popo"
    runner_class: Type[Runner] = SingleThreadedRunner
    num_participants = 3

    def setUp(self):
        eventsourcing.utils._topic_cache.clear()
        self.system = PaxosSystem(KeyValueReplica, self.num_participants)
        self.runner = self.create_runner(
            {
                "PERSISTENCE_MODULE": self.persistence_module,
                "AGGREGATE_CACHE_MAXSIZE": 500,
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
        datastore.close_all_connections()
        super().setUp()


class TestSystemSingleThreaded(KeyValueSystemTestCase):
    @retry((AssertionError, CommandRejected), max_attempts=100, wait=0.05)
    def assert_result(self, app, cmd_text, expected_result):
        self.assertEqual(app.propose_command(cmd_text).result(), expected_result)

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

    @retry(AssertionError, max_attempts=100, wait=0.01)
    def assert_elected_leader_uid(self, app: StateMachineReplica, expected_value):
        self.assertEqual(app.elected_leader_uid, expected_value)

    def test_leader_lease(self):
        apps = [
            self.get_app(f"{KeyValueReplica.__name__}{i}")
            for i in range(self.num_participants)
        ]

        # Configure leadership election mechanism.
        for app in apps:
            app.probability_leader_fails_to_propose_reelection = 0.5
            app.leader_lease = self.num_participants

        # Propose leader.
        # apps[0].propose_leader()
        # sleep(1)
        apps[1].propose_leader()

        # # Wait for leadership election to complete.
        # for app in apps:
        #     self.assert_elected_leader_uid(app, apps[0].name)

        error_count = 0
        result_count = 0

        started = time()
        run_for_duration = 60

        last_period_started = started

        for i in count():
            app = random.choice(apps)
            # app = apps[1]
            # print("Proposing cmd to", app.name)
            try:
                app.propose_command(f'HSET myhash{i} field "Hello World"').result(
                    timeout=1
                )
            except TimeoutError as e:
                error_count += 1
                print(
                    f"Timed out waiting for command result {i}.",
                    "Error count:",
                    error_count,
                )
            else:
                result_count += 1
                period = 50
                if result_count % period == 0:

                    last_period_ended = time()
                    print(
                        f"Got result from app {app.name}.",
                        "Results:",
                        result_count,
                        "Errors:",
                        error_count,
                        "Rate:",
                        f"{period / (last_period_ended - last_period_started):.2f}",
                    )
                    last_period_started = last_period_ended

            # End run after specified duration.
            if time() - started > run_for_duration:
                break


class TestSystemSingleThreadedWithSQLite(TestWithSQLite, TestSystemSingleThreaded):
    pass


class TestSystemSingleThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestSystemSingleThreaded
):
    pass


class TestSystemMultiThreaded(TestSystemSingleThreaded):
    runner_class = MultiThreadedRunner

    def test_leader_lease(self):
        super().test_leader_lease()


class TestSystemMultiThreadedWithSQLite(TestWithSQLite, TestSystemMultiThreaded):
    pass


class TestSystemMultiThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestSystemMultiThreaded
):
    def test_propose_leader(self):
        super().test_propose_leader()


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
            except TimeoutError:
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
    target_rate = 2

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
                app = apps[i % self.num_participants]
                # app = random.choice(apps)
                # app = apps[0]

                # app.assume_leader = True

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
                except CommandRejected as e:
                    print("Command rejected:", e, future.original_cmd_text)
                except CommandFutureEvicted as e:
                    print("Command future evicted:", e, future.original_cmd_text)
                except TimeoutError:
                    raise Exception(
                        "Timeout waiting for future", future.original_cmd_text
                    )
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
        sleep(0.1)

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
    target_rate = 40

    def test_performance(self):
        super().test_performance()


class TestPerformanceMultiThreadedWithPostgreSQL(
    TestWithPostgreSQL, TestPerformanceMultiThreaded
):
    target_rate = 25


del KeyValueSystemTestCase
