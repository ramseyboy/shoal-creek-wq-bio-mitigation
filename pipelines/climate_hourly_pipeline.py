import os
from typing import Union

import geopandas
import pandas
from shapely.wkt import loads

from pipelines import Transformable, Queryable

geopandas.options.io_engine = "pyogrio"


class ClimateHourlyQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
			with
		    climate_table as (
		    	select * from staging."3675312"
		    )
            select encode((date_time || station_number || "parameter")::bytea, 'hex') as ckey,
                   date_time::timestamp,
                   '-97.7604' as longitude, 
                   '30.3208' as latitude,
                   station_number,
                   value,
                   "parameter",
                   ST_Transform(ST_SetSRID(ST_MakePoint('-97.7604'::float, '30.3208'::float), 4326), 26914) as geometry
            from (
	              SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyDewPointTemperature", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Dew Point Temperature'     as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyWetBulbTemperature", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Wet Bulb Temperature'     as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyDryBulbTemperature", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Dry Bulb Temperature'     as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyRelativeHumidity", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Relative Humidity'      as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyPrecipitation", '[^0-9]', '', 'g'), ''), 0.0::text)::float as value,
                         'Precipitation'      as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyVisibility", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Hourly Visibility'      as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyWindDirection", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Wind Direction'      as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyWindSpeed", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Wind Speed'      as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlyWindGustSpeed", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Wind Gust Speed'      as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  union
                  SELECT r."DATE"                   as date_time,
                         r."STATION"                as station_number,
                         coalesce(nullif(regexp_replace(r."HourlySeaLevelPressure", '[^0-9]+', '', 'g'), ''), 0.0::text)::float as value,
                         'Sea Level Pressure'      as "parameter",
                         r."REPORT_TYPE" as "REPORT_TYPE",
                         r."SOURCE" as "SOURCE"
                  FROM climate_table r
                  )
            where ("REPORT_TYPE" = 'FM-15')
          order by date_time;
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_file(filename=self.db_path, sql=self.query_str)


class ClimateHourlyTransformable(Transformable):
    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        df = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(x=df["longitude"], y=df["latitude"],
                                                                               crs="EPSG:4326"))
        df['date_time'] = pandas.to_datetime(df['date_time'])
        df['value'] = pandas.to_numeric(df['value'], errors='coerce').fillna(0.0)

        climate = df.to_crs(crs="EPSG:26914")
        return climate
