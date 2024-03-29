from os import path

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