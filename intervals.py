import geopandas
import pandas
from sqlalchemy import create_engine
from config import PostgresReadOnlyConfig, PostgresReadWriteConfig
import pandas

conn_str = PostgresReadOnlyConfig().__str__()
engine = create_engine(conn_str)

location_2222 = "Shoal Creek @ Shoal Edge Court (EII)"
location_24th = "Shoal Creek @ 24th Street"
location_12th = "USGS-08156800"
location_1st_upstream = "Shoal Creek Upstream of 1st St"

default_interval = 90

parameters_map = {
    "ammonia": ["Ammonia and ammonium", "AMMONIA AS N"],
    "turbidity": ["Turbidity", "TURBIDITY"],
    "conductivity": ["Specific conductance", "CONDUCTIVITY"],
    "tss": ["Total suspended solids", "TOTAL SUSPENDED SOLIDS"],
    "ecoli": ["E COLI BACTERIA", "FECAL COLIFORM BACTERIA", "Fecal Coliform", "Escherichia coli"]
}

parameters = [
    "ammonia",
    "turbidity",
    "conductivity",
    "tss",
    "ecoli"
]

units = {
    "ammonia": "mg/l",
    "turbidity": "NTU",
    "conductivity": "uS/cm",
    "tss": "mg/L",
    "ecoli": "cfu/100ml"
}

def query(parameter: str, sample_location: str):
    query_template = f"""
        SELECT
            max(wq.sample_location) as sample_location,
            max(wq.geometry) as geometry,
            max(wq."parameter") as "parameter",
            max(wq.avg_value) as avg_value,
            max(wq.median_value) as median_value,
            max(wq.max_value) as max_value,
            max(wq.min_value) as min_value,
            max(wq.unit) as unit,
            wq.start_date,
            wq.end_date
        FROM public.water_quality_intervals wq
        where wq."parameter" in ('{parameter}')
            and wq.sample_location in ('{sample_location}')
        group by wq.start_date, wq.end_date
        order by wq.start_date desc;
    """
    conn_str = PostgresReadOnlyConfig().__str__()
    engine = create_engine(conn_str)
    return geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")


def query_with_discharge_precip(parameter: str, sample_location: str):
    query_template = f"""
        SELECT
            max(wq.sample_location) as sample_location,
            max(wq.geometry) as geometry,
            max(wq."parameter") as "parameter",
            max(wq.avg_value) as avg_value,
            max(wq.median_value) as median_value,
            max(wq.max_value) as max_value,
            max(wq.min_value) as min_value,
            max(wq.unit) as unit,
            wq.start_date,
            wq.end_date,
            avg(dd.avg_value) as avg_value_discharge,
            max(dd.avg_value) as max_value_discharge,
            avg(cd.value) as avg_value_precip,
            max(cd.value) as max_value_precip
        FROM public.water_quality_intervals wq
        left outer join public.discharge_daily dd on dd.date_time::date >= wq.start_date and dd.date_time::date < wq.end_date
            and dd."parameter" = 'Discharge'
            and dd.site_number = '08156800'
        left outer join public.climate_daily cd  on cd.date_time::date >= wq.start_date and cd.date_time::date < wq.end_date
            and cd."parameter" = 'Precipitation'
        where wq."parameter" in ('{parameter}')
            and wq.sample_location in ('{sample_location}')
        group by wq.start_date, wq.end_date
        order by wq.start_date desc;
    """
    conn_str = PostgresReadOnlyConfig().__str__()
    engine = create_engine(conn_str)
    return geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")


def __water_quality_interval_query(parameters: list[str], location: str, parameter: str, unit: str,
                                   interval: int = 120) -> pandas.DataFrame:
    parameters_csv = ','.join(f"'{p}'" for p in parameters)

    query_template = f"""
        with 
        sample_date as (
             select wq.sample_date_time::date as sample_date from public.water_quality wq 
             where parameter IN ({parameters_csv})
                and wq.sample_location in ('{location}')
        ),
        intervals as (
            select 
                (select min(s.sample_date) from sample_date s) + ( n    || ' day')::interval start_date,
                (select min(s.sample_date) from sample_date s) + ((n+{interval}) || ' day')::interval end_date
              from generate_series(
              0, 
              ((select max(s.sample_date)::date from sample_date s) - (select min(s.sample_date)::date from sample_date s)),
              {interval}) n
          )
          SELECT 
            max(sample_location) as sample_location,
            max(wq.geometry) as geometry,
            max("parameter") as "parameter",
            avg(wq.value) as avg_value,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wq.value) as median_value,
            max(wq.value) as max_value,
            min(wq.value) as min_value,
            max(wq.unit) as unit,
           i.start_date, 
           i.end_date
           FROM intervals i
           left outer join public.water_quality wq on wq.sample_date_time::date >= i.start_date and wq.sample_date_time::date < i.end_date  
               and parameter IN ({parameters_csv})
                and wq.sample_location in ('{location}')
            group by i.start_date, i.end_date
            order by i.start_date desc;
    """

    df = geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")
    df["unit"] = unit
    df["parameter"] = parameter
    df["sample_location"] = location
    return df


def __write_intervals():
    for sample_location in [location_2222, location_24th, location_12th, location_1st_upstream]:
        wq_intervals = geopandas.GeoDataFrame()
        for filter_parameter in list(units.keys()):
            wq_df = __water_quality_interval_query(parameters=parameters_map[filter_parameter], location=sample_location,
                                                   unit=units[filter_parameter], parameter=filter_parameter,
                                                   interval=default_interval)
            wq_intervals = pandas.concat([wq_intervals, wq_df])
        engine = create_engine(PostgresReadWriteConfig().__str__())
        # wq_intervals.to_postgis("water_quality_intervals", con=engine, if_exists='append')
