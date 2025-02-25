from pydantic_settings import BaseSettings


class EnvConfig(BaseSettings):
    layer_dir: str
    host: str
    port: int



env_config = EnvConfig()




