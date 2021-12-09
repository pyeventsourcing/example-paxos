import datetime
import os
import random
import unittest
from time import time
from typing import Type, cast
from uuid import uuid4

import eventsourcing.utils
from eventsourcing.application import AggregateNotFound
from eventsourcing.persistence import PersistenceError
from eventsourcing.postgres import PostgresDatastore
from eventsourcing.system import MultiThreadedRunner, Runner, SingleThreadedRunner
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


class TestPaxosSystemSingleThreaded(unittest.TestCase):
    runner_class: Type[Runner] = SingleThreadedRunner

    def setUp(self):
        eventsourcing.utils._topic_cache.clear()
        print(self.__class__.__qualname__)
        # Use the same system object in all tests.
        self.system = PaxosSystem(PaxosApplication, 3)
        self.runner = SingleThreadedRunner(self.system)
        # self.runner = MultiThreadedRunner(self.system)
        self.runner.start()

    def tearDown(self):
        self.runner.stop()
        eventsourcing.utils._topic_cache.clear()

    def test_propose_value(self):

        key1, key2, key3 = uuid4(), uuid4(), uuid4()
        value1, value2, value3 = 11111, 22222, 33333

        paxosapplication0 = self.get_paxos_app("PaxosApplication0")
        paxosapplication1 = self.get_paxos_app("PaxosApplication1")
        paxosapplication2 = self.get_paxos_app("PaxosApplication2")

        # Propose value from app 0.
        started1 = datetime.datetime.now()
        assert isinstance(paxosapplication0, PaxosApplication)
        paxosapplication0.propose_value(key1, value1)

        # Check each process has expected final value.
        self.assert_final_value(paxosapplication0, key1, value1)
        self.assert_final_value(paxosapplication1, key1, value1)
        self.assert_final_value(paxosapplication2, key1, value1)
        ended1 = (datetime.datetime.now() - started1).total_seconds()
        print("Resolved paxos proposal 1 in %ss" % ended1)

        # Propose value from app 1.
        started2 = datetime.datetime.now()
        paxosapplication1.propose_value(key2, value2)

        # Check each process has a resolution.
        self.assert_final_value(paxosapplication0, key2, value2)
        self.assert_final_value(paxosapplication1, key2, value2)
        self.assert_final_value(paxosapplication2, key2, value2)
        ended2 = (datetime.datetime.now() - started2).total_seconds()
        print("Resolved paxos proposal 2 in %ss" % ended2)

        # Propose value from app 2.
        started3 = datetime.datetime.now()
        paxosapplication2.propose_value(key3, value3)

        # Check each process has a resolution.
        self.assert_final_value(paxosapplication0, key3, value3)
        self.assert_final_value(paxosapplication1, key3, value3)
        self.assert_final_value(paxosapplication2, key3, value3)
        ended3 = (datetime.datetime.now() - started3).total_seconds()
        print("Resolved paxos proposal 3 in %ss" % ended3)

    def get_paxos_app(self, application_name) -> PaxosApplication:
        return cast(PaxosApplication, self.runner.apps.get(application_name))

    @retry((AggregateNotFound, AssertionError), max_attempts=100, wait=0.05, stall=0)
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
            app.propose_value(key, value)

        # Check each process has a resolution.
        for key, value in keys_and_values.items():
            self.assert_final_value(app0, key, value)
            self.assert_final_value(app1, key, value)
            self.assert_final_value(app2, key, value)

        duration = time() - started

        print(
            f"Resolved {n} paxos proposals in {duration:.3f}s: {duration/n:.3f}s per item; {n/duration:.2f} per sec"
        )


class TestPaxosSystemMultiThreaded(TestPaxosSystemSingleThreaded):
    runner_class = MultiThreadedRunner
