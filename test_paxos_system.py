import datetime
import os
import random
import unittest
from time import time
from typing import cast
from uuid import uuid4

from eventsourcing.application import AggregateNotFound
from eventsourcing.persistence import PersistenceError
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.system import MultiThreadedRunner, SingleThreadedRunner
from eventsourcing.utils import retry

from paxossystem.application import PaxosApplication
from paxossystem.system import PaxosSystem


def drop_postgres_table(datastore: PostgresDatastore, table_name):
    try:
        with datastore.transaction(commit=True) as t:
            statement = f"DROP TABLE {table_name};"
            with t.c.cursor() as c:
                c.execute(statement)
    except PersistenceError:
        pass


class TestPaxosSystem(unittest.TestCase):
    def setUp(self):
        # Use the same system object in all tests.

        # os.environ['INFRASTRUCTURE_FACTORY'] = 'eventsourcing.postgres:Factory'
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
        # drop_postgres_table(db, "paxosapplication0_events")
        # drop_postgres_table(db, "paxosapplication0_snapshots")
        # drop_postgres_table(db, "paxosapplication0_tracking")
        # drop_postgres_table(db, "paxosapplication1_events")
        # drop_postgres_table(db, "paxosapplication1_snapshots")
        # drop_postgres_table(db, "paxosapplication1_tracking")
        # drop_postgres_table(db, "paxosapplication2_events")
        # drop_postgres_table(db, "paxosapplication2_snapshots")
        # drop_postgres_table(db, "paxosapplication2_tracking")
        #
        #
        # os.environ['INFRASTRUCTURE_FACTORY'] = 'eventsourcing.sqlite:Factory'
        # os.environ['PAXOSAPPLICATION0_SQLITE_DBNAME'] = 'file:application0?mode=memory&cache=shared'
        # os.environ['PAXOSAPPLICATION1_SQLITE_DBNAME'] = 'file:application1?mode=memory&cache=shared'
        # os.environ['PAXOSAPPLICATION2_SQLITE_DBNAME'] = 'file:application2?mode=memory&cache=shared'

        # os.environ['COMPRESSOR_TOPIC'] = 'zlib'
        self.system = PaxosSystem(PaxosApplication, 3)
        # self.runner = SingleThreadedRunner(self.system)
        self.runner = MultiThreadedRunner(self.system)
        self.runner.start()

    def tearDown(self):
        self.runner.stop()

    def test_propose_value(self):

        key1, key2, key3 = uuid4(), uuid4(), uuid4()
        value1, value2, value3 = 11111, 22222, 33333

        paxosapplication0 = self.get_paxos_app("PaxosApplication0")
        paxosapplication1 = self.get_paxos_app("PaxosApplication1")
        paxosapplication2 = self.get_paxos_app("PaxosApplication2")

        started1 = datetime.datetime.now()
        assert isinstance(paxosapplication0, PaxosApplication)
        paxosapplication0.propose_value(key1, value1)
        ended1 = (datetime.datetime.now() - started1).total_seconds()

        # Check each process has expected final value.
        self.assert_final_value(paxosapplication0, key1, value1)
        self.assert_final_value(paxosapplication1, key1, value1)
        self.assert_final_value(paxosapplication2, key1, value1)
        print("Resolved paxos proposal 1 with single thread in %ss" % ended1)

        started2 = datetime.datetime.now()
        paxosapplication1.propose_value(key2, value2)
        ended2 = (datetime.datetime.now() - started2).total_seconds()

        # Check each process has a resolution.
        self.assert_final_value(paxosapplication0, key2, value2)
        self.assert_final_value(paxosapplication1, key2, value2)
        self.assert_final_value(paxosapplication2, key2, value2)
        print("Resolved paxos proposal 2 with single thread in %ss" % ended2)

        started3 = datetime.datetime.now()
        paxosapplication2.propose_value(key3, value3)
        ended3 = (datetime.datetime.now() - started3).total_seconds()

        # Check each process has a resolution.
        self.assert_final_value(paxosapplication0, key3, value3)
        self.assert_final_value(paxosapplication1, key3, value3)
        self.assert_final_value(paxosapplication2, key3, value3)
        print("Resolved paxos proposal 3 with single thread in %ss" % ended3)

    def get_paxos_app(self, application_name) -> PaxosApplication:
        return cast(PaxosApplication, self.runner.apps.get(application_name))

    @retry((AggregateNotFound, AssertionError), max_attempts=500, wait=0.01, stall=0)
    def assert_final_value(self, process, id, value):
        self.assertEqual(process.get_final_value(id), value)

    def close_connections_before_forking(self):
        """Implemented by the DjangoTestCase class."""
        pass

    def test_performance(self):
        n = 10
        keys_and_values = {uuid4(): i for i in range(n)}

        app0 = self.get_paxos_app("PaxosApplication0")
        app1 = self.get_paxos_app("PaxosApplication1")
        app2 = self.get_paxos_app("PaxosApplication2")

        started = time()

        for key, value in keys_and_values.items():
            # app = random.choice([app0, app1, app2])
            app = app0
            app.propose_value(key, value, assume_leader=False)

        # Check each process has a resolution.
        for key, value in keys_and_values.items():
            self.assert_final_value(app0, key, value)
            self.assert_final_value(app1, key, value)
            self.assert_final_value(app2, key, value)

        duration = time() - started

        print(
            f"Resolved {n} paxos proposals in {duration}s: {duration/n}s per item; {n/duration} per sec"
        )
