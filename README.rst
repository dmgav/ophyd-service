=============
ophyd-service
=============

Prototype for REST API Server for Ophyd devices


Starting the server::

    uvicorn --host localhost --port 60620 ophyd_service.server:app

Starting the server with config file::

    OPHYD_SERVICE_CONFIG=config.yml uvicorn --host localhost --port 60620 ophyd_service.server:app

Sample config file::

    worker_configuration:
        use_ipython_kernel: true
    authentication:
        single_user_api_key: a

Starting with single user API key::

    OPHYD_SERVICE_SINGLE_USER_API_KEY=a uvicorn --host localhost --port 60620 ophyd_service.server:app

The server still has the concept of environment that currently has to be manually opened before 
the devices become available. To open and close the environment use::

    http POST http://localhost:60620/api/environment/open 'Authorization: ApiKey a'
    http POST http://localhost:60620/api/environment/close 'Authorization: ApiKey a'

The API can be accessed as following:: 

    http GET http://localhost:60620/api/ping 'Authorization: ApiKey a'

The API that reads a device::    

    http GET http://localhost:60620/api/device/read/sim_periodic_device/sine 'Authorization: ApiKey a'


Running helper script for monitoring status::

    python status_monitor.py --api-key a

Running helper script for monitoring devices (devices are available in the demo profile)::

    python stream_monitor.py --api-key a --devices sim_periodic_device.noise sim_periodic_device.sine
    python stream_monitor.py --api-key a --devices rand_async_device2.value
    python stream_monitor.py --api-key a --devices rand_async_device1.value rand_async_device2.value

