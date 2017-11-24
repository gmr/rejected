Running Examples
================
Follow these steps to run the examples:

1. Ensure [RabbitMQ](https://rabbitmq.com) is running on localhost and that it can be connected to using the user `guest` with the password `guest`.
2. Grab [examples](https://github.com/gmr/rejected/tree/master/examples) directory from the [rejected source](https://github.com/gmr/rejected)
3. Create a [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/) and install rejected:
   ```bash
   cd examples
   python3 -m venv env
   . env/bin/activate
   pip install -r ../requires/testing.txt
   ```
   You should see output similar to the following:
   ```
    Collecting beautifulsoup4 (from -r ../requires/testing.txt (line 1))
      Using cached beautifulsoup4-4.6.0-py3-none-any.whl
    ... lots of other pypi output ...
    Successfully installed beautifulsoup4-4.6.0 coverage-4.4.2 helper-2.4.2 mock-2.0.0 nose-1.3.7 pbr-3.1.1 pika-0.11.0 psutil-5.4.1 pyyaml-3.12 raven-6.3.0 six-1.11.0 sprockets-influxdb-2.1.0 tornado-4.5.2 u-msgpack-python-2.4.1
   ```
4. Install rejected from the top-level directory:
   ```bash
   cd ..
   python3 setup.py develop
   ```
   You should see output similar to the following:   
   ```
    running develop
    running egg_info
    creating rejected.egg-info
    ... lots of setup.py develop output ...
    Using /path/to/rejected/examples/env/lib/python3.5/site-packages
    Finished processing dependencies for rejected==4.0.0
   ```
   
3. Run the setup script to configure RabbitMQ and publish test messages in the examples directory:
   ```bash
    cd examples   
   python3 setup.py
   ```
   You should see output similar to the following:
   ```bash
    Declared the examples exchange
    Declared and bound sync_example
    Declared and bound async_example
    Published 100 messages for the sync example
    Published one seed message for the async example
   ```
4. Run the sync example consumer in the examples directory:
   ```bash
   rejected -c examples.yaml -f -p . -o sync
   ```
   You should see output similar to the following:
   ```
   INFO       2017-11-24 18:00:22 6694   examples                  rejected.mcp         __init__                 : rejected v4.0.0 initializing
   INFO       2017-11-24 18:00:22 6694   examples                  rejected.mcp         start_process            : Spawning sync-1 process for sync
   INFO       2017-11-24 18:00:22 6701   sync-1                    examples             process                  : Message: {'Application': {'log_stats': True, 'name': 'Example', 'poll_interval': 10.0, 'value': 24819}} {CID b9269836-6515-41e9-8ae0-9954fe741744}
   INFO       2017-11-24 18:00:22 6701   sync-1                    examples             process                  : Message: {'Application': {'log_stats': True, 'name': 'Example', 'poll_interval': 10.0, 'value': 883}} {CID 875bb2e9-9b81-4b0f-83de-c393a30420bd}
   INFO       2017-11-24 18:00:22 6701   sync-1                    examples             process                  : Message: <html><head><title>Hi</title></head><body>Hello 27188</body></html> {CID 844691d5-ac06-48c2-a99f-f59cb5dd8fdc}
   INFO       2017-11-24 18:00:22 6701   sync-1                    examples             process                  : Message: <html><head><title>Hi</title></head><body>Hello 16352</body></html> {CID 8f4fe8b8-3c84-4c1d-a199-8dcfa53d2768}
   INFO       2017-11-24 18:00:22 6701   sync-1                    examples             process                  : Message: <?xml version="1.0"><document><node><item>True</item><other attr="foo">Bar</other><value>27772</value></node></document> {CID d8690bd5-4115-4fbd-aea6-09e11246d7b5}  
   ```
   When it's stopped, shutdown rejected with ``CTRL-C``:
   ```
   INFO       2017-11-24 18:00:49 6694   examples                  rejected.controller  run                      : Caught CTRL-C, shutting down
   INFO       2017-11-24 18:00:49 6694   examples                  rejected.controller  stop                     : Shutting down controller
   INFO       2017-11-24 18:00:49 6694   examples                  rejected.mcp         stop_processes           : Stopping consumer processes
   INFO       2017-11-24 18:00:49 6694   examples                  rejected.mcp         stop_processes           : Sending SIGABRT to active children
   INFO       2017-11-24 18:00:49 6694   examples                  rejected.mcp         stop_processes           : Waiting on 1 active processes to shut down (0/10)
   INFO       2017-11-24 18:00:49 6694   examples                  rejected.controller  stop                     : MCP exited cleanly
   INFO       2017-11-24 18:00:49 6694   examples                  rejected.controller  stop                     : Shutdown complete
   ```
4. Run the async example consumer in the examples directory:
   ```bash
   rejected -c examples.yaml -f -p . -o async
   ```
   You should see output similar to the following:
   ```
   INFO       2017-11-24 18:02:22 7283   examples                  rejected.mcp         __init__                 : rejected v4.0.0 initializing
   INFO       2017-11-24 18:02:22 7283   examples                  rejected.mcp         start_process            : Spawning async-1 process for async
   INFO       2017-11-24 18:02:23 7290   async-1                   examples             process                  : Length: [47017, 127240] {CID 5afe9893-160f-44dd-8b7f-4f9bf7f73d28}
   INFO       2017-11-24 18:02:23 7290   async-1                   examples             process                  : Pre sleep {CID 5afe9893-160f-44dd-8b7f-4f9bf7f73d28}
   INFO       2017-11-24 18:02:24 7290   async-1                   examples             process                  : Post sleep {CID 5afe9893-160f-44dd-8b7f-4f9bf7f73d28}
   INFO       2017-11-24 18:02:24 7290   async-1                   examples             process                  : Length: [47971, 127240] {CID c4534b58-17b1-4216-acec-81e94372d063}
   INFO       2017-11-24 18:02:24 7290   async-1                   examples             process                  : Pre sleep {CID c4534b58-17b1-4216-acec-81e94372d063}
   INFO       2017-11-24 18:02:25 7290   async-1                   examples             process                  : Post sleep {CID c4534b58-17b1-4216-acec-81e94372d063}   ```
   ```
   When it's stopped, shutdown rejected with ``CTRL-C``:
   ```
   INFO       2017-11-24 18:02:26 7283   examples                  rejected.controller  run                      : Caught CTRL-C, shutting down
   INFO       2017-11-24 18:02:26 7283   examples                  rejected.controller  stop                     : Shutting down controller
   INFO       2017-11-24 18:02:26 7283   examples                  rejected.mcp         stop_processes           : Stopping consumer processes
   INFO       2017-11-24 18:02:26 7283   examples                  rejected.mcp         stop_processes           : Sending SIGABRT to active children
   INFO       2017-11-24 18:02:26 7283   examples                  rejected.mcp         stop_processes           : Waiting on 1 active processes to shut down (0/10)
   INFO       2017-11-24 18:02:26 7283   examples                  rejected.controller  stop                     : MCP exited cleanly
   INFO       2017-11-24 18:02:26 7283   examples                  rejected.controller  stop                     : Shutdown complete   
   ```
