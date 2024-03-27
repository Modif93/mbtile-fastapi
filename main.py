from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response

from mbtiles import MBTileSet

osm_mbtile_set = MBTileSet(mbtiles="/layers/osm-south-korea.mbtiles")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/osm/{z}/{x}/{y}.pbf")
async def get_osm(z: int, x: int, y: int):
    tile = osm_mbtile_set.get_tile(z,x,y)
    tile_pbf = tile.get_pbf()
    return Response(content=tile_pbf, media_type='application/octet-stream')

@app.get("/osm/metadata")
async def get_osm(z: int, x: int, y: int):
    tile = osm_mbtile_set.get_metadata()
    tile_pbf = tile.get_pbf()
    return Response(content=tile_pbf, media_type='application/octet-stream')
