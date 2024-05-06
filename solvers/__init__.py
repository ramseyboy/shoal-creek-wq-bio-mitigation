from enum import Enum


class ChannelType(Enum):
    Trapezoid = 1,
    Rectangle = 2


class RunoffSolverType(Enum):
    Simple = 1,
    SCS = 2
