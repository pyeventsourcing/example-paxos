import os
import random
import unittest
from threading import Thread
from time import sleep, time
from typing import cast

from eventsourcing.application import AggregateNotFound
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.system import MultiThreadedRunner, SingleThreadedRunner
from eventsourcing.utils import retry

from kvstore.application import CommandRejected, KVStore, KVSystem
from test_paxos_system import drop_postgres_table


class TestKVStore(unittest.TestCase):

    def setUp(self):
        # Use the same system object in all tests.

        os.environ['INFRASTRUCTURE_FACTORY'] = 'eventsourcing.postgres:Factory'
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


        # os.environ['INFRASTRUCTURE_FACTORY'] = 'eventsourcing.sqlite:Factory'
        # os.environ['KVSTORE0_SQLITE_DBNAME'] = 'file:application0?mode=memory&cache=shared'
        # os.environ['KVSTORE1_SQLITE_DBNAME'] = 'file:application1?mode=memory&cache=shared'
        # os.environ['KVSTORE2_SQLITE_DBNAME'] = 'file:application2?mode=memory&cache=shared'
        #
        # os.environ['COMPRESSOR_TOPIC'] = 'zlib'
        self.num_participants = 3
        self.system = KVSystem(num_participants=self.num_participants)
        # self.runner = SingleThreadedRunner(self.system)
        self.runner = MultiThreadedRunner(self.system)
        self.runner.start()

    def tearDown(self):
        self.runner.stop()

    def test_do_command(self):

        app0 = self.get_paxos_app("KVStore0")
        app1 = self.get_paxos_app("KVStore1")
        app2 = self.get_paxos_app("KVStore2")

        # Check each process has expected initial value.
        self.assert_query(app0, 'HGET myhash field', None)
        self.assert_query(app1, 'HGET myhash field', None)
        self.assert_query(app2, 'HGET myhash field', None)

        assert isinstance(app0, KVStore)
        started = time()

        app0.do_command('HSET myhash field "Hello"')

        # with self.assertRaises(CommandRejected):
        #     app0.do_command('HSET myhash field "Hells"')

        # Check each process has expected final value.
        self.assert_query(app0, 'HGET myhash field', 'Hello')

        print("Settled command in %ss" % (time() - started))

        self.assert_query(app1, 'HGET myhash field', 'Hello')
        self.assert_query(app2, 'HGET myhash field', 'Hello')

        # with self.assertRaises(IntegrityError):
        app1.do_command('HSET myhash field "Helloooo"')

        # Check each process has expected final value.
        self.assert_query(app0, 'HGET myhash field', 'Helloooo')
        self.assert_query(app1, 'HGET myhash field', 'Helloooo')
        self.assert_query(app2, 'HGET myhash field', 'Helloooo')

        # Check each process has expected initial value.
        self.assert_query(app0, 'HGET myhash nonfield', None)
        self.assert_query(app1, 'HGET myhash nonfield', None)
        self.assert_query(app2, 'HGET myhash nonfield', None)

    def get_paxos_app(self, name) -> KVStore:
        return cast(KVStore, self.runner.apps.get(name))

    @retry((AggregateNotFound, AssertionError), max_attempts=1000, wait=0.3)
    def assert_query(self, app, cmd, value):
        self.assertEqual(app.do_command(cmd), value)

    def close_connections_before_forking(self):
        """Implemented by the DjangoTestCase class."""
        pass

    def test_performance(self):

        apps = [self.get_paxos_app(f"KVStore{i}") for i in range(self.num_participants)]

        n = 1000

        def write():
            for i in range(n):
                app = apps[i % self.num_participants]
                # app = random.choice(apps)
                # app = app1
                app.do_command(f'HSET myhash{i} field "Hello{i}"')
                sleep(0.01)

        started = time()

        def read():
            # Check each process has a resolution.
            for i in range(n):
                app = apps[i % self.num_participants]
                # app = random.choice(apps)
                # app = app0
                self.assert_query(app, f"HGET myhash{i} field", f"Hello{i}")
                duration = time() - started
                c = i + 1
                print(f"Resolved {c} in {duration}s: {duration/c}s per item; {c/duration} per sec")

        # write()

        thread_write = Thread(target=write, daemon=True)
        thread_read = Thread(target=read, daemon=True)
        #
        thread_read.start()
        thread_write.start()
        thread_write.join()
        thread_read.join()

        read()


