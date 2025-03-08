"""
Takes the RHEAS Soil.sql and creates a DSSAT .SOL file

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
    soil_file_lines = ["*SOILS: General DSSAT Soil Input File", ""]
    for row_line in tqdm(lines_str[start_copying_index:]):
        idx, geom_bytes_str, soil_str = row_line.split('\t')
        cur.execute("SELECT ST_AsEWKT(%s)", (geom_bytes_str, ))
        point, = cur.fetchone()
        lon, lat = list(map(float, point[point.find("(")+1:point.find(")")].split()))
        soil_str = soil_str.replace('\\r\\n', '\n').replace('\r', '\n')
        soil_lines = soil_str.split('\n')
        soil_lines[0] = f'*{soil_lines[0][1:]}'
        soil_lines.insert(1, "@SITE        COUNTRY          LAT     LONG SCS FAMILY")
        soil_lines[2] = f'   AL             US     {lat:8.3f} {lon:8.3f} SCS FAMILY'
        soil_lines.insert(3, "@ SCOM  SALB  SLU1  SLDR  SLRO  SLNF  SLPF  SMHB  SMPX  SMKE")
        soil_lines[4] = soil_lines[4][1:]
        soil_lines.insert(
            5,
            "@  SLB  SLMH  SLLL  SDUL  SSAT  SRGF  SSKS  SBDM  SLOC  SLCL  SLSI  SLCF  SLNI  SLHW  SLHB  SCEC  SADC"
        )        
        soil_file_lines += soil_lines
        
    with open('SOIL.SOL', 'w') as f:
        f.writelines('\n'.join(soil_file_lines))
        
cur.close()
con.close()