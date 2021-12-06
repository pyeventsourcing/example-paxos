import itertools
from typing import Type, cast

from eventsourcing.system import System

from paxossystem.application import PaxosApplication


class PaxosSystem(System):
    def __init__(self, app_class: Type[PaxosApplication], num_participants: int):
        self.app_class = app_class
        num_participants = num_participants
        classes = [
            cast(
                Type[PaxosApplication],
                type(
                    f"{self.app_class.name}{i}",
                    (self.app_class,),
                    {"num_participants": num_participants},
                ),
            )
            for i in range(num_participants)
        ]
        assert num_participants > 1
        pipes = [[c[0], c[1], c[0]] for c in itertools.combinations(classes, 2)]

        # pipes = [
        #     [classes[0], classes[1], classes[0]],
        #     [classes[0], classes[2], classes[0]],
        # ]

        # pipes = [
        #     [classes[0], classes[1], classes[0]],
        #     [classes[0], classes[2]],
        # ]

        # # Can't do this, because the protocol phase 1 doesn't work.
        # pipes = [
        #     [classes[0], classes[1]],
        #     [classes[1], classes[2]],
        #     [classes[2], classes[0]],
        # ]
        super(PaxosSystem, self).__init__(pipes)
