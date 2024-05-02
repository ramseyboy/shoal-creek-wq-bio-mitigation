from enum import Enum


class ChannelType(Enum):
    Trapezoid = 1,
    Rectangle = 2


class RunoffSolverType(Enum):
    Rational = 1,
    SCS = 2
