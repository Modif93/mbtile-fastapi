import uvicorn
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    from src.config import env_config

    uvicorn.run("src.application:app", host=env_config.host, port=env_config.port)
