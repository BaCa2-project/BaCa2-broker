from broker.master import BrokerMaster
from settings import APP_SETTINGS, DB_STRING, SUBMITS_DIR


def main():
    broker_instance = BrokerMaster(DB_STRING, SUBMITS_DIR, APP_SETTINGS['delete_records'])
    print('BaCa2 broker is running')
    broker_instance.serve_until_interrupted()
    print('BaCa2 broker has stopped running')


if __name__ == '__main__':
    main()
