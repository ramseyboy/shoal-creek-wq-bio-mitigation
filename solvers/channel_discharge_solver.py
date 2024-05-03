import math

from solvers import ChannelType


class ChannelDischargeSolver:

    def __init__(self, channel_type=ChannelType.Trapezoid, manning_roughness_coefficient: float = None,
                 channel_slope: float = None,
                 bottom_width_channel: float = None, depth_flow: float = None, side_slope: float = None):
        self.side_slope = side_slope
        self.depth_flow = depth_flow
        self.bottom_width_channel = bottom_width_channel
        self.channel_slope = channel_slope
        self.manning_roughness_coefficient = manning_roughness_coefficient
        self.channel_type = channel_type

    def solve(self):
        if self.channel_type == ChannelType.Trapezoid:
            return self.__solve_trapezoidal()
        else:
            raise ValueError(f'Channel type {self.channel_type} not supported')

    def __solve_trapezoidal(self):

        self.ca = self.__cross_sectional_area(self.bottom_width_channel, self.depth_flow, self.side_slope)
        self.wp = self.__wetted_perimeter(self.bottom_width_channel, self.depth_flow, self.side_slope)

        self.hr = self.__hydraulic_radius(self.ca, self.wp)

        self.v = self.__velocity(self.hr, self.channel_slope, self.manning_roughness_coefficient)

        self.q = round(self.__discharge(self.v, self.ca), 2)
        return self.q

    @staticmethod
    def __cross_sectional_area(bottom_width_channel: float, depth_flow: float, side_slope: float) -> float:
        return bottom_width_channel * depth_flow + side_slope * math.pow(depth_flow, 2)

    @staticmethod
    def __wetted_perimeter(bottom_width_channel: float, depth_flow: float, side_slope: float) -> float:
        return bottom_width_channel + (2 * depth_flow) * math.pow((1 + math.pow(side_slope, 2)), 0.5)

    @staticmethod
    def __hydraulic_radius(cross_sectional_area: float, wetted_perimeter: float) -> float:
        return cross_sectional_area / wetted_perimeter

    @staticmethod
    def __velocity(hydraulic_radius: float, channel_slope: float, manning_roughness_coefficient: float) -> float:
        return (1.49 * math.pow(hydraulic_radius, 2 / 3) * math.pow(channel_slope,
                                                                    1 / 2)) / manning_roughness_coefficient

    @staticmethod
    def __discharge(velocity: float, cross_sectional_area: float) -> float:
        return velocity * cross_sectional_area
