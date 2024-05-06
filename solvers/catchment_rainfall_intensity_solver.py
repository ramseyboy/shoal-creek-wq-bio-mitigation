
class CatchmentRainfallIntensitySolver:

    def __init__(self,
                 peak_discharge: float = None,
                 runoff_coefficient: float = None,
                 catchment_area: float = None):
        self.catchment_area = catchment_area
        self.peak_discharge = peak_discharge
        self.runoff_coefficient = runoff_coefficient

    def solve(self) -> float:
        return self.__solve_rational()

    def __solve_rational(self) -> float:
        return round(self.peak_discharge / (self.runoff_coefficient * self.catchment_area), 1)
