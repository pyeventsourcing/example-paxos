from keyvaluestore.domainmodel import KeyValueAggregate, KeyNameIndex
from replicatedstatemachine.application import StateMachineReplica


class KeyValueReplica(StateMachineReplica):
    snapshotting_intervals = {
        KeyValueAggregate: 100,
        KeyNameIndex: 100,
    }
    env = {
        "COMMAND_CLASS": "keyvaluestore.commands:KeyValueCommand",
    }
