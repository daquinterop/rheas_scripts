"""
Repaces the soils in the RHEAS database with the soils provided. This will only
replace the soil points in the provided soils.

Diego Quintero
SERVIR-SCO
"""

import psycopg2 as pg
from osgeo import gdal, ogr
from collections import namedtuple
import numpy as np
from scipy import interpolate
from tqdm import tqdm

import gzip

con = pg.connect(database="rheas")
cur = con.cursor()
with gzip.open('ALsoils.sql.gz','r') as f:
    lines_byte = f.readlines()
    lines_str = [l.decode('utf-8') for l in lines_byte]
    
    # Line where the copying command starts
    start_copying_line = list(filter(lambda x: "FROM stdin;" in x, lines_str))[0]
    start_copying_index = lines_str.index(start_copying_line) + 1
    
    for row_line in tqdm(lines_str[start_copying_index:]):
        idx, geom_bytes_str, soil_str = row_line.split('\t')
        # Remove existing soil if geometry intercepts the new geometry
        query = """
            DELETE FROM dssat.soils 
            WHERE ST_Intersects(geom, (
                SELECT ST_GeomFromEWKT(
                    ST_AsEWKT(%s)
                ) As geom
            ))
        ;
        """
        cur.execute(query, (geom_bytes_str, ))
        # Insert new soil
        query = """
            INSERT INTO dssat.soils (geom, props)
            VALUES (ST_GeomFromEWKT(ST_AsEWKT(%s)), %s)
        ;
        """
        cur.execute(query, (geom_bytes_str, soil_str, ))
        con.commit()
        
cur.close()
con.close()