import os
from typing import Union

import geopandas
import pandas
import shapely
from shapely.wkt import loads

from pipelines import Transformable, Queryable

geopandas.options.io_engine = "pyogrio"


class BioControlsQueryable(Queryable):

    def __init__(self):
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/staging.gpkg")

        self.query_str = """
            select hex(date || OBJECTID) as ckey, * from (select sw.OBJECTID,
                      sw.the_geom              as geometry,
                      sw.DATE_BUILT            as date,
                      sw.CONTROL_TYPE          as type,
                      sw.PROJECT_NAME          as name,
                      sw.CONTROL_AREA_ACRES    as acres,
                      sw.CONTROL_VOLUME        as volume,
                      sw.CONTROL_DEPTH         as depth,
                      sw.DRAINAGE_AREA_ACRES   as drainage_acres,
                      sw.RUNOFF_CAPTURE_DEPTH  as runoff_depth,
                      sw.SHAPE_Area            as geom_area,
                      sw.SHAPE_Length          as geom_length,
                      sw.WATER_QUALITY_CONTROL as is_water_quality_control,
                      sw.WATER_QUALITY_CONTROL as is_flood_control,
                      sw.WATER_QUALITY_CONTROL as is_subsurface_control
               from StormWaterControls_COA as sw
               where sw.STATUS = 'ACTIVE'
               union
               select gz.OBJECTID,
                      gz.the_geom     as geometry,
                      gz.YEAR_START || '-01-01T00:00:00'   as date,
                      gz.MAINTENANCE  as type,
                      gz.LOCATION     as name,
                      gz.ACREAGE      as acres,
                      null            as volume,
                      null            as depth,
                      null            as drainage_acres,
                      null            as runoff_depth,
                      gz.SHAPE_Area   as geom_area,
                      gz.SHAPE_Length as geom_length,
                      'T'             as is_water_quality_control,
                      'F'             as is_flood_control,
                      'F'             as is_subsurface_control
               from GrowZones_COA as gz
               where gz.STATUS = 'ACTIVE'
               union
               select er.fid        as OBJECTID,
                      er.the_geom   as geometry,
                      coalesce(nullif(er.CONSTRUCTI, ''), er.CONSTRUC_1)  as date,
                      er.SOLUTION   as type,
                      er.PROJECT_NA as name,
                      null          as acres,
                      null          as volume,
                      null          as depth,
                      null          as drainage_acres,
                      null          as runoff_depth,
                      er.SHAPE_Area as geom_area,
                      er.SHAPE_LEN  as geom_length,
                      'T'           as is_water_quality_control,
                      'F'           as is_flood_control,
                      'F'           as is_subsurface_control
               from ErosionProjects_COA as er
               where PROJECT_PH = 'PROJECT COMPLETE'
              ) order by date desc
        """

    def query(self) -> Union[geopandas.GeoDataFrame, pandas.DataFrame]:
        return geopandas.read_file(filename=self.db_path, sql=self.query_str)


class BioControlsTransformable(Transformable):

    def __init__(self, boundary_geometry: shapely.Geometry):
        self.boundary_geometry = boundary_geometry

    def transform(self, df: Union[geopandas.GeoDataFrame, pandas.DataFrame]) -> geopandas.GeoDataFrame:
        df = geopandas.GeoDataFrame(df)
        df.geometry = df['geometry'].apply(loads)

        controls = geopandas.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        controls = controls.to_crs("EPSG:26914")

        watershed_controls = controls[controls.intersects(self.boundary_geometry)]
        watershed_controls["type"].unique()

        vegetation_controls_list = ["Wildflower Meadow", "Grow Zone", "BIOFILTRATION", "RAIN_GARDEN",
                                    "VEGETATIVE_FILTER_STRIP", "WET_POND", "FILTRATION_ONLY",
                                    "MSE with limestone boulder toe and vegetated geogrids"]

        return watershed_controls[watershed_controls["type"].isin(vegetation_controls_list)]
