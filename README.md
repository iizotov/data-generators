# Azure Event Hub and Blob Storage Sensor Data Generators
Simple Python 3 data generators for Azure Event Hubs and Blob Storage that generates sensor-like telemetry with the occasionally missing 'pressure' field and stores it as line-delimited JSON files (in ~10M chunks) in Azure Blob Storage as well as pushes it into an Event Hub, row by row.

Sample data (notice a missing field in the second message)
```json
{"deviceId": "tenant1-1", "temperature": 23.371484970077994, "pressure": 472.80611710691255, "ts": "2017-07-18T13:37:30.713834", "source": "upload"}
{"deviceId": "tenant1-5", "temperature": -36.17143911056116, "ts": "2017-07-18T13:37:30.714261", "source": "upload"}
```
## How To Use
1. `pip install azure`
2. replace `#REPLACEME` with your values
3. tweak the parameters - for instance, data chunk size and sleep time. The Event Hub script runs in a multithreaded non-blocking way so it can generate some significant volume. By default it generates 10megs' worth of json and sleeps for a minute
4. `python ./insert_blob.py`
5. `python ./insert_eh.py`

Enjoy! :-)
