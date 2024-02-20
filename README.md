# BaCa2 Broker

BaCa2 Broker is an HTTP server that listens for incoming requests build using FastAPI framework.
It is responsible for managing the communication between BaCa2 and Kolejka checking system - 
it receives tasks from BaCa2, sends them to Kolejka, and then sends the results back to BaCa2.

## Structure

The application is contained in the `app` directory. It consists of the following modules:
- `main.py` - the main module that starts the server
- `handlers.py` - contains handlers for incoming requests that are ment to run in the background.
- `logger.py` - logger logic for the application
- `broker` - a package that contains the logic for the broker, it consists of:
  * `datamaster.py` - logic for managing data
  * `messenger.py` - responsible for sending and receiving messages from/to Kolejka and BaCa2
  * `builder.py` - parses data for Kolejka
  * `master.py` - combines all of the above to manage the whole process

In the `judges` directory there are judge configurations for Kolejka system.

## Running

Broker uses `settings.py` for configuration and can be launched using `run.py` script.
Certain preferences have to be set using environment variables listed in `.end.template` file.
`kolejka.conf` file has to be present in the root directory of the broker and list login
credentials for Kolejka system.
