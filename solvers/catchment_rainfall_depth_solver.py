from solvers import RunoffSolverType


class CatchmentRainfallDepthSolver:

    def __init__(self,
                 runoff_volume: float = None,
                 impervious_cover_fraction: float = None,
                 catchment_area: float = None,
                 total_capacity_with_dry_soil: float = None,
                 total_capacity_with_wet_soil: float = None):
        self.catchment_area = catchment_area
        self.runoff_volume = runoff_volume
        self.impervious_cover_fraction = impervious_cover_fraction
        self.total_capacity_with_dry_soil = total_capacity_with_dry_soil
        self.total_capacity_with_wet_soil = total_capacity_with_wet_soil

    def solve(self) -> tuple[float, float, float]:
        return self.__solve_simple()

    @staticmethod
    def __runoff_coefficient(impervious_cover_fraction: float) -> float:
        return 0.05 + 0.9 * impervious_cover_fraction

    def __solve_simple(self) -> tuple[float, float, float]:
        rc = self.__runoff_coefficient(self.impervious_cover_fraction)
        d = self.runoff_volume / (3630 * rc * self.catchment_area)
        dsd = self.total_capacity_with_dry_soil / (3630 * rc * self.catchment_area)
        dsw = self.total_capacity_with_wet_soil / (3630 * rc * self.catchment_area)
        return round(d, 2), round(dsd, 2), round(dsw, 2)
