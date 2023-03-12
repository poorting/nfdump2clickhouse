# nfdump2clickhouse

Tooling to insert [nfcapd](https://github.com/phaag/nfdump) files into a clickhouse database.

# Introduction


## Setting up clickhouse
### server
You can follow the [setup instructions](https://clickhouse.com/docs/en/install/#self-managed-install) at [clickhouse.com](https://clickhouse.com/) to setup a clickhouse server.

Alternatively - if you have docker installed - you can spin up a clickhouse docker container by issuing a ``docker compose up -d`` command in this directory. The resulting clickhouse instance will have no password set, but it is only reachable from the localhost.

### client
Follow the [instructions](https://clickhouse.com/docs/en/install/#available-installation-options) to install from DEB or RPM packages, but only install the client package (e.g. ``sudo apt-get install -y clickhouse-client``).

If you have the server (container) running, the client can be started with ``clickhouse-client`` and should connect to the clickhouse server automatically. 
typing 

### creating the database


