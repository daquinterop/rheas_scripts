"""
Repaces the planting dates in the RHEAS database with the planting dates provided
by the user. This can also be used with other other raster variables like corpmask.

Diego Quintero
SERVIR-SCO
"""

import psycopg2 as pg
from osgeo import gdal, ogr
from collections import namedtuple
import numpy as np
from scipy import interpolate

import subprocess

DBNAME = "test_rheas"
NEW_RASTER = "/home/dquintero/RHEAS_last/tests/susantha/nt_pd.tif"
TABLE = "crops.plantstart"
con = pg.connect(database=DBNAME)

# A Custom class to save raster info
Raster = namedtuple("Raster", ["crs", "geotransform", "data", "rid", "path"])

# Open new raster and get bounding box
ds = gdal.Open(NEW_RASTER)
arr = ds.GetRasterBand(1).ReadAsArray()
arr = np.where(arr==0, -99, arr) # Set no data as -99
new_rast = Raster(
    crs=ds.GetSpatialRef().GetName(),
    geotransform=ds.GetGeoTransform(),
    data=arr,
    rid=0,
    path=NEW_RASTER
)
ulx, xres, xskew, uly, yskew, yres  = new_rast.geotransform
ysize, xsize = new_rast.data.shape
lrx = ulx + (xsize * xres)
lry = uly + (ysize * yres)
bb_wkt = f"POLYGON (({ulx} {uly}, {lrx} {uly}, {lrx} {lry}, {ulx} {lry}, {ulx} {uly}))"

# Get rid for rasters that overlap new raster
TYPE = "rice"
sql = f"""
    SELECT rid FROM {TABLE} 
    WHERE 
        ST_Intersects(ST_ConvexHull(rast), ST_PolygonFromText('{bb_wkt}', 4326))
        AND type='{TYPE}';
    """
cur = con.cursor()
cur.execute(sql)
rows = cur.fetchall()
rid_list = [r[0] for r in rows]

# Export old rasters to tif files
for rid in rid_list:
    sql = f"""
        DROP TABLE IF EXISTS tmp_out ;
        
        CREATE TABLE tmp_out AS
        SELECT lo_from_bytea(0,
            ST_AsGDALRaster(ST_Union(rast), 'GTiff')
                ) AS loid
        FROM {TABLE} WHERE rid={rid};
        
        SELECT lo_export(loid, '/tmp/old_{rid}.tif')FROM tmp_out;
        
        SELECT lo_unlink(loid) FROM tmp_out
        ;"""
    cur.execute(sql)
    con.commit()

# Open and save data of old rasters
rasters = {}
for rid in rid_list:
    sql = f"""
        SELECT ST_AsGDALRaster(rast, 'GTiff') As rasttif
        FROM {TABLE} WHERE rid={rid};
        """
    cur.execute(sql)
    raster_path = f'/tmp/old_{rid}.tif'
    ds = gdal.Open(raster_path)
    rasters[rid] = Raster(
        crs=ds.GetSpatialRef().GetName(),
        geotransform=ds.GetGeoTransform(),
        data=ds.GetRasterBand(1).ReadAsArray(),
        rid=rid,
        path=raster_path
    )
    # Close and clean up virtual memory file
    ds = band = None
    gdal.Unlink(raster_path)

def get_raster_meshgrid(rast):
    """
    Creates a meshgrid from the raster geotransform
    """
    ulx, xres, xskew, uly, yskew, yres = rast.geotransform
    ysize, xsize = rast.data.shape
    x = ulx + np.arange(0, xsize)*xres + xres/2
    y = uly + np.arange(0, ysize)*yres + yres/2
    return np.meshgrid(x, y)

# Create an NN interpolator.
xx, yy = get_raster_meshgrid(new_rast)
interp = interpolate.RegularGridInterpolator(
    points=(yy[:,0], xx[0]),
    values=new_rast.data,
    method="nearest",
    bounds_error=False,
    fill_value=-99
)

# Replace the old raster values with those of the new raster
# The raster size and resolution won't change. The value of the pixels that overlap the new raster will be 
# replace with the values of the new raster.
new_rasters = {}
for rid, old_rast in rasters.items():
    xx, yy = get_raster_meshgrid(old_rast)
    old_data = old_rast.data
    new_data = interp((yy.flatten(), xx.flatten()))
    new_data = np.where(new_data == -99, old_data.flatten(), new_data)
    new_data = new_data.reshape(old_data.shape)
    print(f"rid {rid}\t{(new_data != old_data).sum()} pixels changed")
    new_rasters[rid] = Raster(
        crs=old_rast.crs,
        geotransform=old_rast.geotransform,
        data=new_data[:],
        rid=rid,
        path=f"/tmp/new_{rid}.tif"
    )

# Create new rasters
for rid, new_rast in new_rasters.items():
    old_rast = rasters[rid]
    driver = gdal.GetDriverByName('GTiff')
    src_ds = gdal.Open(old_rast.path)
    dst_ds = driver.CreateCopy(new_rast.path, src_ds, 0)
    dst_ds.GetRasterBand(1).WriteArray(new_rast.data)
    print(f"{new_rast.path} created")
    src_ds = dst_ds = None

# Replace old rasters with new rasters
for rid, new_rast in new_rasters.items():
    cmd = f"raster2pgsql -d -s 4326 {new_rast.path} temptable | psql -d {DBNAME}"
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = proc.communicate()
    sql = f"""
        UPDATE {TABLE} b
        SET rast=a.rast
        FROM temptable a
            WHERE a.rid=1
            AND b.rid={rid};
        """
    print(f"raster {rid} in {TABLE} was replaced")
    cur.execute(sql)
    con.commit()

con.close()