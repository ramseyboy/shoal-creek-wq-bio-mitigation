from enum import Enum

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

default_interval = 120

parameters_map = {
    "temperature": ["Temperature, water", "Temperature, sample", "WATER TEMPERATURE"],
    "phosphorus": [
        "PHOSPHORUS AS P",
        "ORTHOPHOSPHORUS AS P",
        "Orthophosphate",
        "Orthophosphate",
        "Orthophosphate",
        "PHOSPHATE AS PO4",
        "Phosphorus"
    ],
    "nitrates": [
        "NITRATE/NITRITE AS N",
        "NITRATE AS N",
        "Nitrate",
        "Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)",
        "Organic Nitrogen",
        "Inorganic nitrogen (nitrate and nitrite)",
        "Inorganic nitrogen (nitrate and nitrite) ***retired***use Nitrate + Nitrite"
    ],
    "ph": ["PH", "pH"],
    "ammonia": ["Ammonia and ammonium", "AMMONIA AS N", "Ammonia"],
    "turbidity": ["Turbidity", "TURBIDITY"],
    "conductivity": ["Specific conductance", "CONDUCTIVITY"],
    "tss": ["Total suspended solids", "TOTAL SUSPENDED SOLIDS"],
    "tds": ["TOTAL DISSOLVED SOLIDS", "Total dissolved solids"],
    "ecoli": ["E COLI BACTERIA", "FECAL COLIFORM BACTERIA", "Fecal Coliform", "Escherichia coli", "Total Coliform"]
}

parameters_units_map = {
    "temperature": ["deg C", "Deg. Celsius"],
    "nitrates": ["mg/l as N", "mg/L", "mg/l asNO3", "MG/L", "mg/l asNO2", "mg/l NO3", "mg/l"],
    "phosphorus": ["mg/L", "mg/l as P", "mg/l asPO4", "MG/L", "mg/l", "mg/l as P", "mg/l PO4"],
    "ph": ["std units", "Standard units", "None"],
    "ammonia": ["mg/L", "mg/L", "MG/L", "mg/l NH4", "mg/l as N"],
    "turbidity": ["NTU", "None"],
    "conductivity": ["uS/cm", "uS/cm @25C"],
    "tss": ["mg/L", "Parts Per Million (PPM)", "MG/L", "mg/l"],
    "tds": ["MG/L", "Parts Per Million (PPM)", "mg/L", "mg/l"],
    "ecoli": ["MPN/100ML", "Colonies/100mL", "#/100mL", "cfu/100ml"]
}

parameters = [
    "temperature",
    "phosphorus",
    "nitrates",
    "ph",
    "ammonia",
    "turbidity",
    "conductivity",
    "tss",
    "tds",
    "ecoli"
]

units = {
    "temperature": "deg. C",
    "nitrates": "mg/l",
    "phosphorus": "mg/l",
    "ph": "standard units",
    "ammonia": "mg/l",
    "turbidity": "NTU",
    "conductivity": "uS/cm",
    "tss": "mg/L",
    "tds": "mg/L",
    "ecoli": "cfu/100ml"
}


class Interval(Enum):
    DAILY = 1
    QUADRIMESTER = 1


def query_parameters_before_after_construction(parameters: list[str]):
    parameters_csv = ','.join(f"'{p}'" for p in parameters)
    query_template = f"""
    select 'watershed' as location, min(wq.start_date) as min_date, max(wq."parameter") as "parameter", avg(wq.avg_value) avg_value, stddev(wq.avg_value) stddev, max(wq.max_value) max_value, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wq.median_value) as median_value, max(wq.geometry) as geometry
    from public.water_quality_daily_intervals wq
    where wq.start_date > '2000/01/01' and wq."parameter" in ({parameters_csv})
    group by wq."parameter" , wq.start_date > '2013/09/01'
    order by wq."parameter", min(wq.start_date) desc;
    """
    conn_str = PostgresReadOnlyConfig().__str__()
    engine = create_engine(conn_str)
    return geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")


def query_parameters_before_after_construction_by_location(parameters: list[str], location: str):
    parameters_csv = ','.join(f"'{p}'" for p in parameters)
    query_template = f"""
    select wq.sample_location as location, min(wq.start_date) as min_date, max(wq."parameter") as "parameter", avg(wq.avg_value) avg_value, stddev(wq.avg_value) stddev, max(wq.max_value) max_value, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wq.median_value) as median_value, max(wq.geometry) as geometry
    from public.water_quality_daily_intervals wq
    where sample_location = '{location}' and wq.start_date > '2000/01/01' and wq."parameter" in ({parameters_csv})
    group by wq.sample_location, wq."parameter" , wq.start_date > '2013/09/01'
    order by wq.sample_location, wq."parameter", min(wq.start_date) desc;
    """
    conn_str = PostgresReadOnlyConfig().__str__()
    engine = create_engine(conn_str)
    return geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")


def joined_parameters_query(out_parameters: list[str], out_locations: list[str], interval: Interval):
    joined = geopandas.GeoDataFrame()
    for index, filter_parameter in enumerate(out_parameters):
        if interval == Interval.DAILY:
            i = query_daily(filter_parameter, out_locations)
        else:
            i = query_quadrimester(filter_parameter, out_locations)

        i = i.drop(['end_date', 'sample_location', 'geometry', 'parameter', 'unit'], axis=1)
        if index == 0:
            joined = pandas.concat([joined, i])
        else:
            joined = joined.merge(i, how='outer', on='start_date',
                                  suffixes=(f'_{out_parameters[index - 1]}', f'_{filter_parameter}'))

        if index == len(out_parameters) - 1:
            joined = joined.rename(columns={
                'avg_value': f'avg_value_{filter_parameter}',
                'median_value': f'median_value_{filter_parameter}',
                'max_value': f'max_value_{filter_parameter}',
                'min_value': f'min_value_{filter_parameter}'})
    return joined


def query_quadrimester(parameter: str, sample_locations: list[str]):
    locations_csv = ','.join(f"'{p}'" for p in sample_locations)
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
            and wq.sample_location in ({locations_csv})
        group by wq.start_date, wq.end_date
        order by wq.start_date desc;
    """
    conn_str = PostgresReadOnlyConfig().__str__()
    engine = create_engine(conn_str)
    return geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")


def query_daily(parameter: str, sample_locations: list[str]):
    locations_csv = ','.join(f"'{p}'" for p in sample_locations)
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
        FROM public.water_quality_daily_intervals wq
        where wq."parameter" in ('{parameter}')
            and wq.sample_location in ({locations_csv})
        group by wq.start_date, wq.end_date
        order by wq.start_date desc;
    """
    conn_str = PostgresReadOnlyConfig().__str__()
    engine = create_engine(conn_str)
    return geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")


def query_with_precip_daily(parameter: str, sample_location: str):
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
            avg(cd.value) as avg_value_precip,
            max(cd.value) as max_value_precip
        FROM public.water_quality_daily_intervals wq
        left outer join public.climate_daily cd  on cd.date_time::date >= wq.start_date - INTERVAL '2 DAY' and cd.date_time::date < wq.end_date + INTERVAL '2 DAY'
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


def locations_query(exclude: str = None):
    query_template = f"""
    select wq.sample_location
    from public.water_quality wq 
    where 
        ST_Intersects(
            wq.geometry, 
            (
                select ST_Buffer
                (
                    (select ST_LineMerge(ST_Union(h.geometry)) from public.hydrography h),
                    25
                )
            )
        )
        and wq.sample_location not in ('{exclude}')
    group by wq.sample_location
    order by wq.sample_location
    """

    df = pandas.read_sql_query(
        sql=query_template,
        con=engine)
    return list(df["sample_location"])


def __water_quality_interval_query(parameters: list[str],
                                   parameters_units: list[str],
                                   sample_location: str,
                                   parameter: str,
                                   unit: str,
                                   interval: int = 120, include_null_intervals=True) -> pandas.DataFrame:
    parameters_csv = ','.join(f"'{p}'" for p in parameters)
    parameters_units_csv = ','.join(f"'{p}'" for p in parameters_units)

    query_template = f"""
    with
    buffered_stream as (
        select wq.*
        from public.water_quality wq 
        where 
            ST_Intersects(
                wq.geometry, 
                (
                    select ST_Buffer
                    (
                        (select ST_LineMerge(ST_Union(h.geometry)) from public.hydrography h),
                        25
                    )
                )
            )
        order by wq.sample_location
    ),
    intervals as (
        select 
            (select min(sample_date_time::date) from buffered_stream s) + ( n    || ' day')::interval start_date,
            (select min(sample_date_time::date) from buffered_stream s) + ((n+{interval}) || ' day')::interval end_date
          from generate_series(
          0, 
          ((select max(sample_date_time::date) from buffered_stream s) - (select min(sample_date_time::date) from buffered_stream s)),
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
       left outer join buffered_stream wq on wq.sample_date_time::date >= i.start_date and wq.sample_date_time::date < i.end_date  
           {'where' if include_null_intervals else 'and'} wq."parameter" in ({parameters_csv})
                and wq.unit in ({parameters_units_csv})
                and wq.sample_location in ('{sample_location}')
        group by i.start_date, i.end_date
        order by i.start_date desc;
        
    """

    df = geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")
    df["unit"] = unit
    df["parameter"] = parameter
    df["sample_location"] = sample_location
    return df


def __water_quality_daily_query(parameters: list[str],
                                parameters_units: list[str],
                                sample_location: str,
                                parameter: str,
                                unit: str) -> pandas.DataFrame:
    parameters_csv = ','.join(f"'{p}'" for p in parameters)
    parameters_units_csv = ','.join(f"'{p}'" for p in parameters_units)

    query_template = f"""
    with
    buffered_stream as (
        select wq.*
        from public.water_quality wq 
        where 
            ST_Intersects(
                wq.geometry, 
                (
                    select ST_Buffer
                    (
                        (select ST_LineMerge(ST_Union(h.geometry)) from public.hydrography h),
                        25
                    )
                )
            )
        order by wq.sample_location
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
       wq.sample_date_time::date as start_date, 
       wq.sample_date_time::date as end_date
       FROM buffered_stream wq where wq."parameter" in ({parameters_csv})
                and wq.unit in ({parameters_units_csv})
                and wq.sample_location in ('{sample_location}')
        group by wq.sample_date_time::date
        order by wq.sample_date_time::date desc;

    """

    df = geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")
    df["unit"] = unit
    df["parameter"] = parameter
    df["sample_location"] = sample_location
    return df


def __write_intervals():
    locations = locations_query()
    engine = create_engine(PostgresReadWriteConfig().__str__())
    location_intervals = geopandas.GeoDataFrame()
    for location in locations:
        wq_intervals = geopandas.GeoDataFrame()
        for filter_parameter in parameters:
            wq_df = __water_quality_interval_query(parameters=parameters_map[filter_parameter],
                                                   parameters_units=parameters_units_map[filter_parameter],
                                                   sample_location=location,
                                                   unit=units[filter_parameter], parameter=filter_parameter,
                                                   interval=default_interval)
            wq_intervals = pandas.concat([wq_intervals, wq_df])
        location_intervals = pandas.concat([location_intervals, wq_intervals])
    location_intervals.to_postgis("water_quality_intervals", con=engine, if_exists='replace')


def __write_days():
    locations = locations_query()
    engine = create_engine(PostgresReadWriteConfig().__str__())
    location_intervals = geopandas.GeoDataFrame()
    for location in locations:
        wq_intervals = geopandas.GeoDataFrame()
        for filter_parameter in parameters:
            wq_df = __water_quality_daily_query(parameters=parameters_map[filter_parameter],
                                                parameters_units=parameters_units_map[filter_parameter],
                                                sample_location=location,
                                                unit=units[filter_parameter], parameter=filter_parameter)
            wq_intervals = pandas.concat([wq_intervals, wq_df])
        location_intervals = pandas.concat([location_intervals, wq_intervals])
    location_intervals.to_postgis("water_quality_daily_intervals", con=engine, if_exists='replace')


if __name__ == '__main__':
    __write_days()
