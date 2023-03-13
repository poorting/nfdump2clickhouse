# nfdump2clickhouse

Tooling to insert [nfcapd](https://github.com/phaag/nfdump) files into a clickhouse database.

# Introduction

Only works on linux. Tested on debian and ubuntu.

# Setting up

## Requirements

nfsen toolchain installed

## Clickhouse
### Server
You can follow the [setup instructions](https://clickhouse.com/docs/en/install/#self-managed-install) at [clickhouse.com](https://clickhouse.com/) to setup a clickhouse server.

Alternatively - if you have docker installed - you can spin up a clickhouse docker container by issuing a ``docker compose up -d`` command in this directory. The resulting clickhouse instance will have no password set, but it is only reachable from the localhost.

### Client
Follow the [instructions](https://clickhouse.com/docs/en/install/#available-installation-options) to install from DEB or RPM packages, but only install the client package (e.g. ``sudo apt-get install -y clickhouse-client``).

If you have the server (container) running, the client can be started with ``clickhouse-client`` and should connect to the clickhouse server automatically. 


### Creating the database and table
Startup the clickhouse client with ``clickhouse-client``, it should connect to the clickhouse database automatically.

Enter the following commands to create the nfsen database and the flows table:
```
CREATE DATABASE nfsen

CREATE TABLE nfsen.flows
(
    `ts` DateTime DEFAULT 0,
    `te` DateTime DEFAULT 0,
    `sa` String,
    `da` String,
    `sp` UInt16 DEFAULT 0,
    `dp` UInt16 DEFAULT 0,
    `pr` Nullable(String),
    `flg` String,
    `ipkt` UInt64,
    `ibyt` UInt64,
    `ra` String,
    `flowsrc` String
)
ENGINE = MergeTree
PARTITION BY tuple()
PRIMARY KEY (ts, te)
ORDER BY (ts, te, sa, da)
TTL te + toIntervalDay(90)
```
These commands are also in the schema.txt file.

If you want to store flows for a different amount of days than 90, adjust the final line accordingly.

## nfdump2clickhouse

### Python virtual environment

### Configuration

### Testing

### Installing as service

``sudo ./install.sh``
