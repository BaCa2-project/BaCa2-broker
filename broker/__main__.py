import uvicorn

from settings import IP, PORT
from .server import app

if __name__ == '__main__':
    uvicorn.run(app, host=IP, port=int(PORT))
