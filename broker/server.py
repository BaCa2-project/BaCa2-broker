from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class Content(BaseModel):
    """Content of baCa2 submit request"""
    submit_id: str
    pass_hash: str
    package_path: str
    commit_id: str
    submit_path: str


@app.post("/kolejka/{submit_id}")
async def kolejka_post(submit_id: int):
    ...


@app.post("/baca")
async def baca_post(content: Content):
    ...
