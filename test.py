import datetime
import os
import unittest
from uuid import uuid4

from eventsourcing.application.sqlalchemy import SQLAlchemyApplication
from eventsourcing.domain.model.decorators import retry
from eventsourcing.domain.model.events import (
    assert_event_handlers_empty,
    clear_event_handlers,
)

from paxos.application import PaxosApplication, PaxosSystem


class TestPaxosSystem(unittest.TestCase):
    # Use the same system object in all tests.
    system = PaxosSystem(setup_tables=True)

    # Use SQLAlchemy infrastructure (override in subclasses).
    infrastructure_class = SQLAlchemyApplication

    # infrastructure_class = PopoApplication

    def test(self):

        key1, key2, key3 = uuid4(), uuid4(), uuid4()
        value1, value2, value3 = 11111, 22222, 33333

        concrete_system = self.system.bind(self.infrastructure_class)
        with concrete_system as runner:
            paxosapplication0 = runner.processes["paxosapplication0"]
            paxosapplication1 = runner.processes["paxosapplication1"]
            paxosapplication2 = runner.processes["paxosapplication2"]

            started1 = datetime.datetime.now()
            assert isinstance(paxosapplication0, PaxosApplication)
            paxosapplication0.propose_value(key1, value1)
            ended1 = (datetime.datetime.now() - started1).total_seconds()

            # Check each process has expected final value.
            self.assert_final_value(paxosapplication0, key1, value1)
            self.assert_final_value(paxosapplication1, key1, value1)
            self.assert_final_value(paxosapplication2, key1, value1)
            print("Resolved paxos 1 with single thread in %ss" % ended1)

            started2 = datetime.datetime.now()
            paxosapplication1.propose_value(key2, value2)
            ended2 = (datetime.datetime.now() - started2).total_seconds()

            # Check each process has a resolution.
            self.assert_final_value(paxosapplication0, key2, value2)
            self.assert_final_value(paxosapplication1, key2, value2)
            self.assert_final_value(paxosapplication2, key2, value2)
            print("Resolved paxos 2 with single thread in %ss" % ended2)

            started3 = datetime.datetime.now()
            paxosapplication2.propose_value(key3, value3)
            ended3 = (datetime.datetime.now() - started3).total_seconds()

            # Check each process has a resolution.
            self.assert_final_value(paxosapplication0, key3, value3)
            self.assert_final_value(paxosapplication1, key3, value3)
            self.assert_final_value(paxosapplication2, key3, value3)
            print("Resolved paxos 3 with single thread in %ss" % ended3)

    @retry((KeyError, AssertionError), max_attempts=100, wait=0.05, stall=0)
    def assert_final_value(self, process, id, value):
        self.assertEqual(process.get_final_value(id), value)

    def close_connections_before_forking(self):
        """Implemented by the DjangoTestCase class."""
        pass

    def setUp(self) -> None:
        assert_event_handlers_empty()

    def tearDown(self):
        try:
            del os.environ["DB_URI"]
        except KeyError:
            pass
        try:
            assert_event_handlers_empty()
        finally:
            clear_event_handlers()
