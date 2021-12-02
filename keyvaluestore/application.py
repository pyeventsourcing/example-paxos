from keyvaluestore.domainmodel import KeyValueAggregate, KeyIndex
from replicatedstatemachine.application import StateMachineReplica


class KeyValueStore(StateMachineReplica):
    snapshotting_intervals = {
        KeyValueAggregate: 100,
        KeyIndex: 100,
    }
