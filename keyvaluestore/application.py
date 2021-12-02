from keyvaluestore.domainmodel import KeyValueAggregate, KeyNameIndex
from replicatedstatemachine.application import StateMachineReplica


class KeyValueStore(StateMachineReplica):
    snapshotting_intervals = {
        KeyValueAggregate: 100,
        KeyNameIndex: 100,
    }
