from os import path

import numpy as np
import rasterio
import rasterio.mask
import shapely


def clip_raster(src_file_path: str, out_file_path: str, clip_geom: shapely.Geometry):
    with rasterio.open(src_file_path) as dem:
        out_image, out_transform = rasterio.mask.mask(dem, [clip_geom], crop=True)
        out_meta = dem.meta

    out_meta.update({"driver": "GTiff",
                     "height": out_image.shape[1],
                     "width": out_image.shape[2],
                     "transform": out_transform})

    with rasterio.open(out_file_path, "w", **out_meta) as dest:
        dest.write(out_image)


def fix_no_data_value(src_file_path: str, out_file_path: str, no_data_value=0):
    with rasterio.open(src_file_path, "r+") as src:
        src.nodata = no_data_value
        with rasterio.open(out_file_path, 'w', **src.profile) as dst:
            for i in range(1, src.count + 1):
                band = src.read(i)
                band = np.where(band == no_data_value, no_data_value, band)
                dst.write(band, i)


if __name__ == '__main__':
    fix_no_data_value("data/shoal_creek_nlcd_land_cover_change.tif", "data/shoal_creek_nlcd_land_cover_change1.tif", no_data_value=0)
    fix_no_data_value("data/shoal_creek_nlcd_imperviousness.tif", "data/shoal_creek_nlcd_imperviousness1.tif", no_data_value=0)
    fix_no_data_value("data/shoal_creek_nlcd_land_cover.tif", "data/shoal_creek_nlcd_land_cover1.tif", no_data_value=0)
