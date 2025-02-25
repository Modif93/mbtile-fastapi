from typing import Any

from pydantic import BaseModel, Field


class MbtileConnection(BaseModel):
    name: str
    format: str
    directory: str
    connection: Any = Field(exclude=True)
