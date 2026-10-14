=============
ophyd-service
=============

Prototype for REST API Server for Ophyd devices


Starting the server::

    uvicorn --host localhost --port 60620 ophyd_service.server:app

Starting the server with config file::

    OPHYD_SERVICE_CONFIG=config.yml uvicorn --host localhost --port 60620 ophyd_service.server:app

Minimum config file::

    authentication:
        single_user_api_key: a

Starting with single user API key::

    OPHYD_SERVICE_SINGLE_USER_API_KEY=a uvicorn --host localhost --port 60620 ophyd_service.server:app


The API can be accessed as following:: 

    http GET http://localhost:60620/api/v1/ping 'Authorization: ApiKey a'

The API that reads a device::    

    http GET http://localhost:60620/api/v1/device/read/sim_periodic_device1 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/get/sim_periodic_device1 'Authorization: ApiKey a'

    http GET http://localhost:60620/api/v1/device/read/sim_periodic_device1/sine 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/get/sim_periodic_device1/sine 'Authorization: ApiKey a'

    http GET http://localhost:60620/api/v1/device/read/rand_async_device1 'Authorization: ApiKey a'

    http GET http://localhost:60620/api/v1/device/read/rand_async_device1/value 'Authorization: ApiKey a'
    http GET http://localhost:60620/api/v1/device/get/rand_async_device1/value 'Authorization: ApiKey a'


