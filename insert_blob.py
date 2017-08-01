from azure.storage.blob import ContentSettings, BlockBlobService
from random import randrange, random, choice
from datetime import datetime
import json
import time
import sys
from threading import Thread
from queue import Queue


def upload(i, q):
    print("thread {} started".format(i))

    while True:
        work_item = q.get()
        contents = work_item[0]
        path = work_item[1]
        block_blob_service.create_blob_from_text(container_name, 
            path, 
            contents, 
            content_settings=ContentSettings(content_type='application/json'), 
            max_connections=10)
        q.task_done()
        print("\nthread {}, upload {} complete".format(i, path))

num_threads = 5
sleep_time = 60.0
max_size = 10 * 1024 * 1024 #10 Meg
tenant = '1'
num_devices = 10;
account_key = '#REPLACEME'
account_name = '#REPLACEME'
block_blob_service = BlockBlobService(account_name = account_name, account_key = account_key)

path =  'raw_input/'
container_name = 'tenant' + tenant

msg_array = ''

task_queue = Queue()

for i in range(num_threads):
    worker = Thread(target=upload, args=(i, task_queue, ))
    #worker.setDaemon(True)
    worker.start()

while True:
    try:
        msg = {}
        is_error = choice([True, False])
        device_id = randrange(1, num_devices)
        temperature = randrange(-50, 50) + random()
        pressure = randrange(0, 500) + random()
        ts = datetime.now().isoformat()
        msg["deviceId"] = "tenant" + tenant + "-" + str(device_id)
        msg["temperature"] = temperature
        if not is_error:
            msg["pressure"] = pressure
        msg["ts"] = ts
        msg["source"] = "upload"
        msg_array += json.dumps(msg) + '\n'

        current_size = len(msg_array)
        progress = int( current_size / max_size * 100)

        if current_size >= max_size:
            dtnow = datetime.utcnow()
            filename = str(dtnow.year) + '/' + str(dtnow.month).zfill(2) + '/' + str(dtnow.day).zfill(2) + '/' + \
                str(dtnow.hour).zfill(2) + '/' + str(int(dtnow.minute) - int(dtnow.minute) % 15).zfill(2) + '/' + \
                str(dtnow.second).zfill(2) + '.' + str(dtnow.microsecond).zfill(5) + ".json"
            task_queue.put((msg_array, path + filename))
            print("\nupload queue size is {}".format(task_queue.qsize()))
            msg_array = ''
            time.sleep(sleep_time)

        
    except Exception as e:
        print(e) 

   
