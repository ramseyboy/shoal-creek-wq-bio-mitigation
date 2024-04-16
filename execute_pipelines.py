from config import PostgresReadWriteConfig
from pipelines import Pipeline
from pipelines.bio_controls_pipeline import BioControlsQueryable, BioControlsTransformable
from pipelines.climate_daily_pipeline import ClimateDailyQueryable, ClimateDailyTransformable
from pipelines.climate_hourly_pipeline import ClimateHourlyTransformable, ClimateHourlyQueryable
from pipelines.discharge_daily_pipeline import DischargeDailyQueryable, DischargeDailyTransformable
from pipelines.discharge_pipeline import DischargeQueryable, DischargeTransformable
from pipelines.hydrography_pipeline import HydrographyQueryable, HydrographyTransformable
from pipelines.water_quality_pipeline import WaterQualityQueryable, WaterQualityTransformable
from pipelines.watershed_pipeline import WatershedQueryable, WatershedTransformable


def export_postgis():
    cnx = PostgresReadWriteConfig().__str__()

    watershed_pipeline = Pipeline() \
        .query(WatershedQueryable()) \
        .transform(WatershedTransformable())

    watershed = watershed_pipeline.dataframe()
    watershed_pipeline.export_postgis(conn_str=cnx, layer_name='watershed')

    Pipeline() \
        .query(HydrographyQueryable()) \
        .transform(HydrographyTransformable(watershed.geometry[0])) \
        .export_postgis(conn_str=cnx, layer_name='hydrography')

    Pipeline() \
        .query(DischargeQueryable()) \
        .transform(DischargeTransformable()) \
        .export_postgis(conn_str=cnx, layer_name='discharge')

    Pipeline() \
        .query(ClimateHourlyQueryable()) \
        .transform(ClimateHourlyTransformable()) \
        .export_postgis(conn_str=cnx, layer_name='climate_hourly')

    Pipeline() \
        .query(DischargeDailyQueryable()) \
        .transform(DischargeDailyTransformable()) \
        .export_postgis(conn_str=cnx, layer_name='discharge_daily')

    Pipeline() \
        .query(ClimateDailyQueryable()) \
        .transform(ClimateDailyTransformable()) \
        .export_postgis(conn_str=cnx, layer_name='climate_daily')

    Pipeline() \
        .query(WaterQualityQueryable()) \
        .transform(WaterQualityTransformable(watershed.geometry[0])) \
        .export_postgis(conn_str=cnx, layer_name='water_quality')

    Pipeline() \
        .query(BioControlsQueryable()) \
        .transform(BioControlsTransformable(watershed.geometry[0])) \
        .export_postgis(conn_str=cnx, layer_name='bio_controls')


def export_geopackage():
    db_path = "data/shoal-creek-wq-bio-mitigation.gpkg"
    watershed_pipeline = Pipeline() \
        .query(WatershedQueryable()) \
        .transform(WatershedTransformable())

    watershed = watershed_pipeline.dataframe()
    watershed_pipeline.export_geopackage(geopackage_path=db_path, layer_name='watershed')

    Pipeline() \
        .query(HydrographyQueryable()) \
        .transform(HydrographyTransformable(watershed.geometry[0])) \
        .export_geopackage(geopackage_path=db_path, layer_name='hydrography')

    Pipeline() \
        .query(DischargeQueryable()) \
        .transform(DischargeTransformable()) \
        .export_geopackage(geopackage_path=db_path, layer_name='discharge')

    Pipeline() \
        .query(ClimateHourlyQueryable()) \
        .transform(ClimateHourlyTransformable()) \
        .export_geopackage(geopackage_path=db_path, layer_name='climate_hourly')

    Pipeline() \
        .query(WaterQualityQueryable()) \
        .transform(WaterQualityTransformable(watershed.geometry[0])) \
        .export_geopackage(geopackage_path=db_path, layer_name='water_quality')

    Pipeline() \
        .query(BioControlsQueryable()) \
        .transform(BioControlsTransformable(watershed.geometry[0])) \
        .export_geopackage(geopackage_path=db_path, layer_name='bio_controls')


def export_parquet():
    watershed_pipeline = Pipeline() \
        .query(WatershedQueryable()) \
        .transform(WatershedTransformable())

    watershed = watershed_pipeline.dataframe()
    watershed_pipeline.export_parquet(layer_name='watershed')

    Pipeline() \
        .query(HydrographyQueryable()) \
        .transform(HydrographyTransformable(watershed.geometry[0])) \
        .export_parquet(layer_name='hydrography')

    Pipeline() \
        .query(DischargeQueryable()) \
        .transform(DischargeTransformable()) \
        .export_parquet(layer_name='discharge')

    Pipeline() \
        .query(ClimateHourlyQueryable()) \
        .transform(ClimateHourlyTransformable()) \
        .export_parquet(layer_name='climate_hourly')

    Pipeline() \
        .query(WaterQualityQueryable()) \
        .transform(WaterQualityTransformable(watershed.geometry[0])) \
        .export_parquet(layer_name='water_quality')

    Pipeline() \
        .query(BioControlsQueryable()) \
        .transform(BioControlsTransformable(watershed.geometry[0])) \
        .export_parquet(layer_name='bio_controls')


if __name__ == '__main__':
    export_postgis()
