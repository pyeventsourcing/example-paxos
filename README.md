# Paxos system, and distributed key-value store

[![Build Status](https://travis-ci.org/johnbywater/es-example-paxos.svg?branch=master)](https://travis-ci.org/johnbywater/es-example-paxos)
[![Coverage Status](https://coveralls.io/repos/github/johnbywater/es-example-paxos/badge.svg?branch=master)](https://coveralls.io/github/johnbywater/es-example-paxos)

This repository has three examples.

* A Paxos system that comes to consensus on proposals for the final value
of a unique key. The system comprises instances of an event-sourced
Paxos application class, each of which have an event-sourced Paxos
aggregate that has an instance of the Paxos protocol. Proposing a value
for a key starts a Paxos aggregate, which announces Paxos messages as
domain events. The domain events are processed by the other applications
in the system, by receiving them into Paxos aggregates, leading to
further messages being announced that are processed by the other
applications. Eventually, consensus is reached across the system.

* A replicated state machine that extends the Paxos system,
and comes to consensus on an ordered sequence of proposed commands,
each of which is executed in the same order on each replica.

* A distributed key-value system that extends the replicated state machine
application by supplying a set of key-value commands that can be 
proposed and executed, and key-value aggregates that evolve according to
those commands. Keys can be created, read, updated, deleted, and renamed.
