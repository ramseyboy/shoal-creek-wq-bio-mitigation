import math

from solvers import ChannelType


class VolumeSolver:

    def __init__(self, channel_type, channel_length: float = None,
                 top_width_channel: float = None, bottom_width_channel: float = None, depth_flow: float = None,
                 soil_depth: float = None, soil_water_capacity: float = None):
        self.soil_water_capacity = soil_water_capacity
        self.soil_depth = soil_depth
        self.depth_flow = depth_flow
        self.bottom_width_channel = bottom_width_channel
        self.top_width_channel = top_width_channel
        self.channel_length = channel_length
        self.channel_type = channel_type

    def solve(self):
        if self.channel_type == ChannelType.Trapezoid:
            return self.__solve_trapezoidal()
        elif self.channel_type == ChannelType.Rectangle:
            return self.__solve_rectangular()
        else:
            raise ValueError(f'Channel type {self.channel_type} not supported')

    def __solve_trapezoidal(self):
        v = ((self.bottom_width_channel + self.top_width_channel) / 2) * self.depth_flow * self.channel_length

        if self.soil_water_capacity is not None and self.soil_depth is not None:
            bottom_area = self.bottom_width_channel * self.channel_length
            soil_volume = bottom_area * self.soil_depth
            available_soil_volume = soil_volume * self.soil_water_capacity
            v = available_soil_volume + v

        return round(v, 2)

    def __solve_rectangular(self):
        v = self.channel_length * self.top_width_channel * self.depth_flow
        return round(v, 2)
