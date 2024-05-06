
class CatchmentPeakDischargeSolver:

    def __init__(self,
                 rainfall_intensity: float = None,
                 runoff_coefficient: float = None,
                 catchment_area: float = None):
        self.catchment_area = catchment_area
        self.rainfall_intensity = rainfall_intensity
        self.runoff_coefficient = runoff_coefficient

    def solve(self) -> float:
        return self.__solve_rational()

    def __solve_rational(self) -> float:
        return round(self.runoff_coefficient * self.rainfall_intensity * self.catchment_area, 1)
