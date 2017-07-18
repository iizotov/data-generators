from azure.servicebus import ServiceBusService
from random import randrange, random, choice
from datetime import datetime
import json
import time

tenant = '1'
num_devices = 10;
shared_access_key_value = '#REPLACEME'
shared_access_key_name = 'RootManageSharedAccessKey'
service_namespace = '#REPLACEME'
sbs = ServiceBusService(service_namespace,
                        shared_access_key_name=shared_access_key_name,
                        shared_access_key_value=shared_access_key_value)
hub_name = 'tenant' + tenant


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
        msg["source"] = "eventhub"

        print(json.dumps(msg))
        sbs.send_event(hub_name, json.dumps(msg))
        time.sleep(0.01)
    except Exception as e:
        print(e) 

   