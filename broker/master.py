from baca2PackageManager.broker_communication import BacaToBroker

from .messenger import KolejkaMessengerInterface, BacaMessengerInterface
from .datamaster import DataMasterInterface

from settings import KOLEJKA_SRC_DIR, APP_SETTINGS


class BrokerMaster:

    def __init__(self,
                 data_master: DataMasterInterface,
                 kolejka_messenger: KolejkaMessengerInterface,
                 baca_messenger: BacaMessengerInterface):
        self.kolejka_messenger = kolejka_messenger
        self.baca_messenger = baca_messenger
        self.data_master = data_master

    async def handle_baca(self, data: BacaToBroker):
        ...

    async def handle_kolejka(self, submit_id: int):
        ...
