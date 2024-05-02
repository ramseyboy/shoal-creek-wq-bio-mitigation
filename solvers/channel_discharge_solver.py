import math

from solvers import ChannelType


class ChannelDischargeSolver:

    def __init__(self, channel_type=ChannelType.Trapezoid, manning_roughness_coefficient: float = None, channel_slope: float = None,
                 bottom_width_channel: float = None, depth_flow: float = None, side_slope: float = None,
                 soil_infiltration_rate: float = None):
        self.soil_infiltration_rate = soil_infiltration_rate
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

        ca = self.__cross_sectional_area(self.bottom_width_channel, self.depth_flow, self.side_slope)
        wp = self.__wetted_perimeter(self.bottom_width_channel, self.depth_flow, self.side_slope)

        hr = self.__hydraulic_radius(ca, wp)

        v = self.__velocity(hr, self.channel_slope, self.manning_roughness_coefficient)

        if self.soil_infiltration_rate is not None:
            pass #todo

        return round(self.__discharge(v, ca), 2)

    @staticmethod
    def __cross_sectional_area(bottom_width_channel: float, depth_flow: float, side_slope: float) -> float:
        return bottom_width_channel * depth_flow + side_slope * math.pow(depth_flow, 2)

    @staticmethod
    def __wetted_perimeter(bottom_width_channel: float, depth_flow: float, side_slope: float) -> float:
        return bottom_width_channel + 2 * depth_flow * (1 + math.pow(side_slope, 2)) * 0.5

    @staticmethod
    def __hydraulic_radius(cross_sectional_area: float, wetted_perimeter: float) -> float:
        return cross_sectional_area / wetted_perimeter

    @staticmethod
    def __velocity(hydraulic_radius: float, channel_slope: float, manning_roughness_coefficient: float) -> float:
        return (1.49 * math.pow(hydraulic_radius, 2 / 3) * math.pow(channel_slope, 1 / 2)) / manning_roughness_coefficient

    @staticmethod
    def __discharge(velocity: float, channel_area: float) -> float:
        return velocity * channel_area
