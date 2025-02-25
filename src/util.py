import asyncio
import gzip
import os
from io import BytesIO
from typing import List

from aiofiles import os as aio_os
from aiosqlite import Connection, connect as connect_sqlite

from .exception import LayerDirectoryNotFound
from .model import MbtileConnection


async def get_path(layer_dir: str):
    is_dir = await aio_os.path.isdir(layer_dir)
    if not is_dir:
        raise LayerDirectoryNotFound
    layer_file_list = await aio_os.listdir(layer_dir)
    return [os.path.join(layer_dir, l) for l in layer_file_list if l.endswith(".mbtiles")]


async def get_metadata(c: Connection):
    cur = await c.execute("SELECT name,value FROM metadata")
    rows = await cur.fetchall()
    metadata = dict()
    for row in rows:
        metadata[row[0]] = row[1]
    return metadata


def get_query(z: int, x: int, y: int):
    return f"SELECT tile_data FROM tiles WHERE zoom_level={z} AND tile_column={x} AND tile_row={y}"


async def get_tile(c: Connection, z: int, x: int, y: int, data_type: str = 'raster'):
    cur = await c.execute(get_query(z, x, y))
    row = await cur.fetchone()
    if not row:
        return None
    if data_type != 'raster':
        raw_data = BytesIO(row[0])
        with gzip.open(raw_data, 'rb') as f:
            tile = f.read()
        return tile
    else:
        return bytes(row[0])


async def get_connection(layer_file: str):
    layer_file_basename = os.path.basename(layer_file)
    layer_name = layer_file_basename.split('.')[0]
    connection = await connect_sqlite(layer_file)
    metadata = await get_metadata(connection)
    layer_format = metadata['format']
    return MbtileConnection(
        name=layer_name,
        directory=layer_file,
        format=layer_format,
        connection=connection
    )


async def get_connections(layer_dir: str) -> List[MbtileConnection]:
    layer_files = await get_path(layer_dir)
    connects = [get_connection(l) for l in layer_files]
    return await asyncio.gather(*connects)


