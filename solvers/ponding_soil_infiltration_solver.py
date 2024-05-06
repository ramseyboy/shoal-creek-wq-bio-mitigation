class PondingSoilInfiltrationSolver:
    def __init__(self,
                 wetted_perimeter: float = None,
                 flow_volume: float = None,
                 channel_length: float = None,
                 soil_infiltration_rate: float = None,
                 soil_depth: float = None,
                 soil_water_capacity_dry: float = None,
                 soil_water_capacity_wet: float = None):
        self.channel_length = channel_length
        self.wetted_perimeter = wetted_perimeter
        self.flow_volume = flow_volume
        self.soil_water_capacity_dry = soil_water_capacity_dry
        self.soil_water_capacity_wet = soil_water_capacity_wet
        self.soil_depth = soil_depth
        self.soil_infiltration_rate = soil_infiltration_rate

    def solve(self) -> tuple[float, float, float, float, float]:
        wetted_area = self.wetted_perimeter * self.channel_length
        soil_volume = wetted_area * self.soil_depth
        available_soil_volume_wet = soil_volume * self.soil_water_capacity_wet
        available_soil_volume_dry = soil_volume * self.soil_water_capacity_dry

        soil_rate_fth = self.soil_infiltration_rate / 12
        soil_rate_cfh = soil_rate_fth * wetted_area

        return round(soil_volume, 1), round(available_soil_volume_dry, 1), round(available_soil_volume_wet, 1), round(soil_rate_cfh, 1), round(self.flow_volume / soil_rate_cfh, 1)
