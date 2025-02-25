from contextlib import asynccontextmanager
from typing import Dict

from fastapi import FastAPI, HTTPException, status,Request,Response
from fastapi.middleware.cors import CORSMiddleware

from .config import env_config
from .model import MbtileConnection
from .util import get_connections, get_metadata, get_tile

layers: Dict[str, MbtileConnection] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global layers
    connections = await get_connections(env_config.layer_dir)
    for c in connections:
        layers[c.name] = c

    yield

    for c in connections:
        await c.connection.close()



app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/settings")
def get_layers():
    return [c for n, c in layers.items()]


@app.get("/{layer}/metadata")
async def get_metadata_json(layer: str,request:Request):
    if layer not in layers.keys():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Layer not Found")

    hostname = request.headers.get('host')
    metadata = await get_metadata(layers[layer].connection)
    layer_path = "{z}/{x}/{y}"
    metadata['tiles'] = [f"http://{hostname}/{layer}/{layer_path}.{layers[layer].format}"]

    return metadata



@app.get("/{layer}/{zoom}/{x}/{y}.{ext}")
async def get_tile_response(layer: str, zoom: int, x: int, y: int, ext: str):
    if layer not in layers.keys():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Layer not Found")
    if layers[layer].format != ext:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not Found")

    if layers[layer].format == 'pbf':
        tile_content = await get_tile(layers[layer].connection, zoom, x, y, 'vector')
    else:
        tile_content = await get_tile(layers[layer].connection, zoom, x, y)

    return Response(content=tile_content, media_type='application/octet-stream')

