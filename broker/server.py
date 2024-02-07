import asyncio

from baca2PackageManager.broker_communication import BacaToBroker, make_hash
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from settings import BROKER_PASSWORD

from .master import BrokerMaster

master = BrokerMaster(  # TODO
    None,
    None,
    None
)
app = FastAPI()


class Content(BaseModel):
    """Content of baCa2 submit request"""
    submit_id: str
    pass_hash: str
    package_path: str
    commit_id: str
    submit_path: str


@app.post("/kolejka/{submit_id}")
async def kolejka_post(submit_id: str):
    """Handle notifications from kolejka"""
    submit_normalized = submit_id.replace('_', '')

    if not submit_normalized.isalnum():
        raise HTTPException(status_code=400)

    await asyncio.create_task(master.handle_kolejka(submit_normalized))

    return {"message": "Success", "status_code": 200}


@app.post("/baca")
async def baca_post(content: Content):
    """Handle submit request from baCa2"""
    broker_password = BROKER_PASSWORD

    btb = BacaToBroker(content.pass_hash,
                       content.submit_id,
                       content.package_path,
                       content.commit_id,
                       content.submit_path)

    if make_hash(broker_password, btb.submit_id) != btb.pass_hash:
        raise HTTPException(status_code=401, detail="Wrong Password")

    await asyncio.create_task(master.handle_baca(btb))

    return {"message": "Success", "status_code": 200}
