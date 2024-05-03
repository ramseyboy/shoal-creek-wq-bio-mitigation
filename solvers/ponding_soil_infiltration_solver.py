class PondingSoilInfiltrationSolver:
    def __init__(self,
                 wetted_perimeter: float = None,
                 flow_volume: float = None,
                 channel_length: float = None,
                 soil_infiltration_rate: float = None,
                 soil_depth: float = None,
                 soil_water_capacity: float = None):
        self.channel_length = channel_length
        self.wetted_perimeter = wetted_perimeter
        self.flow_volume = flow_volume
        self.soil_water_capacity = soil_water_capacity
        self.soil_depth = soil_depth
        self.soil_infiltration_rate = soil_infiltration_rate

    def solve(self):
        wetted_area = self.wetted_perimeter * self.channel_length
        soil_volume = wetted_area * self.soil_depth
        available_soil_volume = soil_volume * self.soil_water_capacity

        soil_rate_fth = self.soil_infiltration_rate / 12
        soil_rate_sfs = soil_rate_fth * wetted_area

        remaining_flow_volume = self.flow_volume

        if remaining_flow_volume <= available_soil_volume:
            return round(remaining_flow_volume / soil_rate_sfs, 2)

        t = 0
        while remaining_flow_volume > 0:
            if remaining_flow_volume > available_soil_volume:
                v_mod = remaining_flow_volume % available_soil_volume
                t * 1.5  # todo: how does reaching water capacity affect further infiltration / evaporation etc..
            else:
                v_mod = available_soil_volume
            t = t + v_mod / soil_rate_sfs
            remaining_flow_volume = remaining_flow_volume - v_mod
        return round(t, 2)
