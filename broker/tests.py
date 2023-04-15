from broker.master import KolejkaCommunicationServer
from http import client
from threading import Thread, Lock
import random as rand
from time import sleep


def test1_kolejka_communication(sub_number=1000, host='127.0.0.1', port=8080, max_wait=0.01) -> int:
    server = KolejkaCommunicationServer(host, port)
    server.start_server()

    submits = []
    awaiting = []
    checked = []

    submits_lock = Lock()
    awaiting_lock = Lock()
    checked_lock = Lock()

    # generate submits
    for i in range(sub_number):
        submits.append('%010d' % i)

    def send_submits():
        while True:
            submits_lock.acquire()
            if len(submits) == 0:
                submits_lock.release()
                break
            cur = submits.pop(rand.randrange(0, len(submits)))
            with awaiting_lock:
                awaiting.append(cur)
            server.add_submit(cur)
            print('%s sent' % cur)
            submits_lock.release()
            sleep(rand.uniform(0, max_wait))

    def check_submits():
        count = 0
        while count < sub_number:
            awaiting_lock.acquire()
            if len(awaiting) == 0:
                awaiting_lock.release()
                sleep(max_wait)
                continue
            cur = awaiting.pop(rand.randrange(0, len(awaiting)))
            awaiting_lock.release()
            cl = client.HTTPConnection(host, port)
            cl.connect()
            cl.request('GET', cur)
            cl.getresponse()
            print('%s checked' % cur)
            with checked_lock:
                checked.append(cur)
            count += 1
            sleep(rand.uniform(0, max_wait))

    def wait_for_one(sub):
        server.await_submit(sub)
        print('%s awaited' % sub)

    def wait_for_submits():
        count = 0
        threads = []
        while count < sub_number:
            checked_lock.acquire()
            if len(checked) == 0:
                checked_lock.release()
                sleep(max_wait)
                continue
            cur = checked.pop(rand.randrange(0, len(checked)))
            checked_lock.release()
            th = Thread(target=wait_for_one, args=[cur])
            threads.append(th)
            th.start()
            count += 1
            sleep(rand.uniform(0, max_wait))
        for thread in threads:
            thread.join()

    send = Thread(target=send_submits)
    check = Thread(target=check_submits)
    wait = Thread(target=wait_for_submits)
    send.start()
    check.start()
    wait.start()

    send.join()
    check.join()
    wait.join()
    server.stop_server()

    # print(submits, awaiting, checked)
    print('server.submit_dict:', server.submit_dict)
    if not any([submits, awaiting, checked, server.submit_dict]):
        print('all good')
        return 0
    else:
        print('fail')
        return 1


if __name__ == '__main__':
    test1_kolejka_communication(max_wait=0.001, sub_number=10000)
