from solvers import RunoffSolverType


class CatchmentRunoffSolver:

    def __init__(self, solver_type: RunoffSolverType = RunoffSolverType.Simple,
                 rainfall_depth: float = None,
                 impervious_cover_fraction: float = None,
                 catchment_area: float = None):
        self.catchment_area = catchment_area
        self.rainfall_depth = rainfall_depth
        self.solver_type = solver_type
        self.impervious_cover_fraction = impervious_cover_fraction

    def solve(self) -> float:
        if self.solver_type == RunoffSolverType.Simple:
            return self.__solve_simple()
        else:
            raise ValueError(f'Solver type {self.solver_type} not supported')

    @staticmethod
    def __runoff_coefficient(impervious_cover_fraction: float) -> float:
        return 0.05 + 0.9 * impervious_cover_fraction

    def __solve_simple(self) -> float:
        rc = self.__runoff_coefficient(self.impervious_cover_fraction)
        return round(3630 * self.rainfall_depth * rc * self.catchment_area, 1)
