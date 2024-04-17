import geopandas
from sqlalchemy import create_engine
from config import PostgresReadOnlyConfig

conn_str = PostgresReadOnlyConfig().__str__()
engine = create_engine(conn_str)


def precipitation_interval_query(parameters: list[str], locations: list[str], parameter: str):
    parameters_csv = ','.join(f"'{p}'" for p in parameters)
    locations_csv = ','.join(f"'{l}'" for l in locations)

    query_template = f"""
    with 
    sample_date as (
     select wq.sample_date_time::date as sample_date from public.water_quality wq 
     where parameter IN ({parameters_csv})
        and wq.sample_location in ({locations_csv})
    ),
    intervals as (
    select 
            (select min(s.sample_date) from sample_date s) + ( n    || ' day')::interval start_date,
            (select min(s.sample_date) from sample_date s) + ((n+120) || ' day')::interval end_date
          from generate_series(
          0, 
          ((select max(s.sample_date)::date from sample_date s) - (select min(s.sample_date)::date from sample_date s)), 120) n
      )
      SELECT 
        max(c.station_number) as station_number,
        max(c.geometry) as geometry,
        max(c."parameter") as "parameter",
        avg(c.value) as avg_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY c.value) as median_value,
        max(c.value) as max_value,
        min(c.value) as min_value,
       i.start_date, 
       i.end_date
       FROM intervals i
       left outer join public.climate_daily c on c.date_time::date >= i.start_date and c.date_time::date < i.end_date 
       and c."parameter" IN ('Precipitation')
        group by i.start_date, i.end_date
        order by i.start_date desc;
    """
    df = geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")
    df["wq_parameter"] = parameter
    return df


def discharge_interval_query(parameters: list[str], locations: list[str], site_number: str, parameter: str):
    parameters_csv = ','.join(f"'{p}'" for p in parameters)
    locations_csv = ','.join(f"'{l}'" for l in locations)

    query_template = f"""
    with
    sample_date as (
     select wq.sample_date_time::date as sample_date from public.water_quality wq 
     where parameter IN ({parameters_csv})
        and wq.sample_location in ({locations_csv})
    ),
    intervals as (
    select 
            (select min(s.sample_date) from sample_date s) + ( n    || ' day')::interval start_date,
            (select min(s.sample_date) from sample_date s) + ((n+120) || ' day')::interval end_date
          from generate_series(
          0, 
          ((select max(s.sample_date)::date from sample_date s) - (select min(s.sample_date)::date from sample_date s)),
          120) n
      )
    select  
    'USGS-' || max(d.site_number) as site_number,
    max(d.geometry) as geometry,
    max(d."parameter") as "parameter",
    avg(d.avg_value) as avg_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d.avg_value) as median_value,
    max(d.avg_value) as max_value,
    min(d.avg_value) as min_value,
    max(d.unit) as unit,
    i.start_date, 
    i.end_date
    from intervals i
       left outer join public.discharge_daily d on d.date_time::date >= i.start_date and d.date_time::date < i.end_date
        and "parameter" = 'Discharge' and d.site_number = '{site_number}'
        group by i.start_date, i.end_date
        order by i.start_date desc;
    """
    df = geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")
    df["wq_parameter"] = parameter
    return df


def water_quality_interval_query(parameters: list[str], locations: list[str], parameter: str, unit: str):
    parameters_csv = ','.join(f"'{p}'" for p in parameters)
    locations_csv = ','.join(f"'{l}'" for l in locations)

    query_template = f"""
        with 
        sample_date as (
             select wq.sample_date_time::date as sample_date from public.water_quality wq 
             where parameter IN ({parameters_csv})
                and wq.sample_location in ({locations_csv})
        ),
        intervals as (
            select 
                (select min(s.sample_date) from sample_date s) + ( n    || ' day')::interval start_date,
                (select min(s.sample_date) from sample_date s) + ((n+120) || ' day')::interval end_date
              from generate_series(
              0, 
              ((select max(s.sample_date)::date from sample_date s) - (select min(s.sample_date)::date from sample_date s)),
              120) n
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
                and wq.sample_location in ({locations_csv})
            group by i.start_date, i.end_date
            order by i.start_date desc;
    """

    df = geopandas.read_postgis(
        sql=query_template,
        con=engine, geom_col='geometry', crs="EPSG:26914")
    df["unit"] = unit
    df["parameter"] = parameter
    return df
