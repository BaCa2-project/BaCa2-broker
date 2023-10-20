from time import sleep

from broker.master import BrokerMaster
from settings import APP_SETTINGS, DB_STRING, SUBMITS_DIR

if __name__ == '__main__':
    broker_instance = BrokerMaster(DB_STRING, SUBMITS_DIR, APP_SETTINGS['delete_records'])
    print('BaCa2 broker is running')
