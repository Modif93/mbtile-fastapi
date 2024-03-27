import gzip
from io import BytesIO
import sqlite3
import zlib
import json
import os
import shutil
from dbutils.pooled_db import PooledDB


class MBTileSet:

    def __init__(self, mbtiles, outdir=None, origin="bottom"):
        self.pool = PooledDB(creator=sqlite3, database=mbtiles, maxconnections=12)
        self.conn = self.pool.connection()

        self.outdir = outdir
        self.origin = origin
        if self.origin not in ['bottom', 'top']:
            raise Exception("origin must be either `bottom` or `top`")

    def get_tile(self, z, x, y):
        return MBTile(z, x, y, self.pool.connection(), self.origin)

    def get_metadata(self):
        c = self.pool.connection().cursor()
        c.execute('''SELECT name, value FROM metadata''')
        rows = c.fetchall()

        metadata = {}
        for row in rows:
            key = row[0]
            value = row[1]
            if key.startswith('json'):
                metadata[key] = json.loads(value)
            else:
                metadata[key] = value
        return metadata


class MBTile:

    def __init__(self, z, x, y, conn, origin):
        self.zoom = z
        self.col = x
        self.row = y
        self.conn = conn
        self.origin = origin
        self.blank_png_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'blank.png')

    def get_raster(self):
        c = self.conn.cursor()
        c.execute('''SELECT tile_data 
                     FROM tiles 
                       WHERE zoom_level = %s 
                       AND tile_column = %s 
                       AND tile_row = %s''' % (self.zoom, self.col, self.row))
        row = c.fetchone()
        if not row:
            return None

        return bytes(row[0])

    def get_vector(self):
        c = self.conn.cursor()
        c.execute('''SELECT tile_data 
                     FROM tiles 
                       WHERE zoom_level = %s 
                       AND tile_column = %s 
                       AND tile_row = %s''' % (self.zoom, self.col, self.row))
        row = c.fetchone()
        if not row:
            return None
        # return row[0]
        raw_data = BytesIO(row[0])
        with gzip.open(raw_data, 'rb') as f:
            tile = f.read()
        return tile
