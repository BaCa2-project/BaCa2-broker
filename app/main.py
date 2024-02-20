import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from baca2PackageManager.broker_communication import BacaToBroker, make_hash
import settings

from .broker.master import BrokerMaster
from .broker.datamaster import DataMaster, SetSubmit, TaskSubmit
from .broker.messenger import KolejkaMessenger, BacaMessenger, PackageManager, KolejkaMessengerActiveWait
from .handlers import PassiveHandler, ActiveHandler
from .logger import LoggerManager


# APP ===================================================================================


logger_manager = LoggerManager(__name__, settings.LOG_FILE, 0)
logger_manager.set_formatter(settings.LOGGER_PROMPT)
logger_manager.start()
logger = logger_manager.logger

data_master = DataMaster(
    task_submit_t=TaskSubmit,
    set_submit_t=SetSubmit,
    logger=logger
)

if settings.ACTIVE_WAIT:
    tmp_t = KolejkaMessengerActiveWait
else:
    tmp_t = KolejkaMessenger

kolejka_messanger = tmp_t(
    submits_dir=settings.SUBMITS_DIR,
    build_namespace=settings.BUILD_NAMESPACE,
    kolejka_conf=settings.KOLEJKA_CONF,
    kolejka_callback_url_prefix=settings.KOLEJKA_CALLBACK_URL_PREFIX,
    logger=logger
)

baca_messanger = BacaMessenger(
    baca_success_url=settings.BACA_RESULTS_URL,
    baca_failure_url=settings.BACA_ERROR_URL,
    password=settings.BACA_PASSWORD,
    logger=logger
)

package_manager = PackageManager(
    kolejka_src_dir=settings.KOLEJKA_SRC_DIR,
    build_namespace=settings.BUILD_NAMESPACE,
    force_rebuild=False,
)

master = BrokerMaster(
    data_master=data_master,
    kolejka_messenger=kolejka_messanger,
    baca_messenger=baca_messanger,
    package_manager=package_manager,
    logger=logger
)

if settings.ACTIVE_WAIT:
    handlers = ActiveHandler(master, master.kolejka_messenger, logger)
else:
    handlers = PassiveHandler(master, logger)

daemons = set()


@asynccontextmanager
async def lifespan(app_: FastAPI):
    # start daemons
    task = asyncio.create_task(data_master.start_daemons(task_submit_timeout=settings.TASK_SUBMIT_TIMEOUT,
                                                         interval=settings.DELETION_DAEMON_INTERVAL))
    daemons.add(task)

    yield

    # stop daemons
    for task in daemons:
        task.cancel()
    await asyncio.gather(*daemons, return_exceptions=True)

    # stop logger
    logger_manager.stop()


app = FastAPI(title='BaCa2-broker', lifespan=lifespan)


# VIEWS =================================================================================

class Content(BaseModel):
    """Content of baCa2 submit request"""
    submit_id: str
    pass_hash: str
    package_path: str
    commit_id: str
    submit_path: str


@app.get("/")
async def root():
    return {"message": "Broker is running"}


@app.post("/baca")
async def baca_post(content: Content, background_tasks: BackgroundTasks):
    """Handle submit request from baCa2"""
    btb = BacaToBroker(content.pass_hash,
                       content.submit_id,
                       content.package_path,
                       content.commit_id,
                       content.submit_path)

    if make_hash(settings.BROKER_PASSWORD, btb.submit_id) != btb.pass_hash:
        raise HTTPException(status_code=401, detail="Wrong Password")

    background_tasks.add_task(handlers.handle_baca, btb)

    return {"message": "Success", "status_code": 200}


@app.post("/kolejka/{submit_id}")
async def kolejka_post(submit_id: str, background_tasks: BackgroundTasks):
    """Handle notifications from kolejka"""
    if settings.ACTIVE_WAIT:
        raise HTTPException(status_code=404, detail="Not Active - broker in 'active wait' mode")

    submit_normalized = submit_id.replace('_', '')

    if not submit_normalized.isalnum():
        raise HTTPException(status_code=400)

    background_tasks.add_task(handlers.handle_kolejka, submit_normalized)

    return {"message": "Success", "status_code": 200}
