import asyncio

from baca2PackageManager.broker_communication import BacaToBroker, make_hash
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from aiologger import Logger
from settings import (BROKER_PASSWORD, SUBMITS_DIR, BUILD_NAMESPACE, KOLEJKA_CONF,
                      KOLEJKA_CALLBACK_URL_PREFIX, BACA_URL, BACA_PASSWORD, KOLEJKA_SRC_DIR)

from .master import BrokerMaster
from .datamaster import DataMaster, SetSubmit, TaskSubmit
from .messenger import KolejkaMessenger, BacaMessenger, PackageManager


logger = Logger.with_default_handlers(name="broker")
broker_password = BROKER_PASSWORD

data_master = DataMaster(
    task_submit_t=TaskSubmit,
    set_submit_t=SetSubmit
)

kolejka_messanger = KolejkaMessenger(
    submits_dir=SUBMITS_DIR,
    build_namespace=BUILD_NAMESPACE,
    kolejka_conf=KOLEJKA_CONF,
    kolejka_callback_url_prefix=KOLEJKA_CALLBACK_URL_PREFIX,
    logger=logger
)

baca_messanger = BacaMessenger(
    baca_url=BACA_URL,
    password=BACA_PASSWORD,
    logger=logger
)

package_manager = PackageManager(
    kolejka_src_dir=KOLEJKA_SRC_DIR,
    build_namespace=BUILD_NAMESPACE,
    force_rebuild=False,
)

master = BrokerMaster(
    data_master=data_master,
    kolejka_messenger=kolejka_messanger,
    baca_messenger=baca_messanger,
    package_manager=package_manager,
    logger=logger
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

    await asyncio.create_task(master.handle_kolejka(submit_normalized), name=submit_normalized)

    return {"message": "Success", "status_code": 200}


@app.post("/baca")
async def baca_post(content: Content):
    """Handle submit request from baCa2"""
    btb = BacaToBroker(content.pass_hash,
                       content.submit_id,
                       content.package_path,
                       content.commit_id,
                       content.submit_path)

    if make_hash(broker_password, btb.submit_id) != btb.pass_hash:
        raise HTTPException(status_code=401, detail="Wrong Password")

    await asyncio.create_task(master.handle_baca(btb), name=btb.submit_id)

    return {"message": "Success", "status_code": 200}
