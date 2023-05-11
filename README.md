# nfdump2clickhouse

Tooling to insert [nfcapd](https://github.com/phaag/nfdump) files into a clickhouse database.

## Introduction
The [nfdump](https://github.com/phaag/nfdump) / [nfsen](https://github.com/phaag/nfsen) combination is widely used to collect, analyze and visualize network traffic. 
Once setup properly it really helps network and/or security teams in their daily work. 
It provides a nice graphical view on network data and allows you to drill down in the data for analysis.

One drawback - at least if used for security analysis - is that it is very slow if you want to analyze historical data over longer periods of time. 
For example: if it turns out at some point that a specific IP address is connected to an 'evil actor', you would like to find out if there has been contact with that IP address over the past X days, weeks or even months. 

But analyzing historical data in those cases with a specific nfdump filter/profile is painfully slow, even with the newer (multi-threaded) version of the toolset. 
This is **not** because of a design flaw in nfdump, but simply because nfdump/nfsen was never designed with that specific use case in mind. The way it works is by aggregating and filtering data as it comes in and storing the results of those, ready for you to view and analyze.  

But if you *know* where to look (or better: *when*), then you can use nfsen to home in on that specific timeslice and use nfsen again to visualize/analyze further. Even if you then do need further filtering to drill down, that filtering will be much quicker because the window of data is already much much smaller!

The question then quickly becomes: is there a better way to hunt down appearances of specific IP address(es) in netflow data so that we can 'target' those time windows with nfdump/nfsen? 

This of course is analogous to finding a needle in a haystack; which is exactly what an analytical database is designed to do: processing large volumes of data either for analysis or for finding specific occurrences. 
In order to do that you need to have the relevant data in an analytical database to start with, which is where nfdump2clickhouse comes in!

It is obvious from the name that [clickhouse](https://clickhouse.com/#getting_started) is the analytical database of choice. Simply because it is easy to install (or dockerize) on a fairly standard (but preferably big and powerful) VM or machine, without the need to setup and manage entire clusters; as some other big(gish) data 'solutions' will have you do (which seems rather silly and over the top if the only goal is to be able to pinpoint timeframes for further analysis). 

## What it does

**nfdump2clickhouse converts raw flow data (nfcapd files) as they come in, into parquet files and inserts those into clickhouse.**  
Just the raw data without aggregation or filtering. No more, no less. That is it. 

The image below (adapted from a [paper describing the principe of operation of nfdump tools](https://www.first.org/resources/papers/conference2006/haag-peter-papers.pdf)), shows the relationship between nfdump/nfcapd, nfdump2clickhouse and clickhouse:

* **nfcapd** receives the netflow input streams from the various sources (*ident1* through *ident3* in the figure) and stores them on disk. It does this in a time based fashion by rotating and renaming the output file every  n minutes (typically 5 minutes) and renaming the output file with the time stamp (*YYYYMMddhhmm*) of the interval. So a file named **nfcapd.202305081205** contains data from the 8th of May 2023 12:05 onwards (for 5 minutes, so data until 12:09:59).

* **nfdump2clickhouse** watches the directories where the nfcapd files are stored for these rotations (renaming actions) and converts those freshly stored files. First to CSV format (using nfdump) and subsequently to parquet format. It then invokes the clickhouse-client to ingest the resulting parquet file into the clickhouse database.

![nfdump2parquet process](n2c-process.png)

Only the fields from the netflow records that are relevant for the goal are converted and ingested into clickhouse. These are listed below, with their data type in the clickhouse database:

|*name*| *description*                                                                                     | *data type* |
|------|---------------------------------------------------------------------------------------------------|-------------|
|ts   | Start time of the flow                                                                            | DateTime    |
|te     | End time of the flow                                                                              | DateTime    |
|sa     | Source IP address                                                                                 | String      |
|da     | Destination IP address                                                                            | String      |
|sp     | Source port                                                                                       | UInt16      |
|dp     | Destination port                                                                                  | UInt16      |
|pr     | Protocol (e.g. 'TCP' or 'UDP')                                                                    | String      |
|flg    | Flags (if pr is 'TCP')                                                                            | String      |
|ipkt   | Number of packets in this flow                                                                    | UInt64      |
|ibyt   | Number of bytes in this flow                                                                      | UInt64      |
|ra     | IP address of the router/network device that exported this flow information                       | String      |
|flowsrc| Additional label added by nfdump2clickhouse. <br/>Can be set in the config file per flow exporter | String      |

Please be aware that since the conversion takes place directly from the nfcapd files stored (before any aggregation and.or filtering), this means that flows are 'one-sided'; so a connection between a webbrowser X and a webserver Y will show up as two flows: one with IP address and port of X as a source and those of Y as a destination, and one flow that covers the other way around. This is also why the *opkt* and *obyt* fields from netflow records aren't stored, since they will always be zero because of this.

In practice this is not a problem, since the purpose is to identify timeframes where communication with a malicious host has taken place. So if you know that some external IP address was abused as a C2 server in a specific timeframe (say last month), you can simply search for all source IP addresses of flows that have that IP address as a destination within the last month.    

### Storage requirements

To get a rough estimate of the amount of space you need to reserve, the rule of thumb is that 100 million flows take roughly 2GB of disk space (50 million flows/GB).

As an example: if your network produces 2 billion (2000 million) flows per day on average, and you want to store 90 days worth of flows, that would work out as (2000/100) x 2 x 90 = 3600GB (roughly 3.5TB, assume 4TB to be on the safe side)

### Caveats
Only works on linux. 

Tested on debian and ubuntu.

The way it is setup now means that the nfdump/nfsen toolset and netflow data need to be on the same machine as nfdump2clickhouse. In practice this need not be a problem if your netflow machine is already big and beefy enough. 

If it needs to be on a separate machine, you can most likely change the invocation of 'clickhouse-client' to connect to a clickhouse database on another server. 

Alternatively you can use a tool such as [samplicator](https://github.com/sleinen/samplicator) to duplicate/reflect netflow stream(s) to multiple destinations, one to your normal setup and one to the new machine specifically for this purpose. Of course then the netflow data needs to be processed by nfdump on the new machine as well, but without the need to store the historical netflow data.

# Setting up

## Requirements

nfsen toolchain installed

### Clickhouse
#### Server
You can follow the [setup instructions](https://clickhouse.com/docs/en/install/#self-managed-install) at [clickhouse.com](https://clickhouse.com/) to setup a clickhouse server.

Alternatively - if you have docker installed - you can spin up a clickhouse docker container by issuing a ``docker compose up -d`` command in this directory. The resulting clickhouse instance will have no password set, but it is only reachable from the localhost.

#### Client
Follow the [instructions](https://clickhouse.com/docs/en/install/#available-installation-options) to install from DEB or RPM packages, but only install the client package (e.g. ``sudo apt-get install -y clickhouse-client``).

If you have the server (container) running, the client can be started with ``clickhouse-client`` and should connect to the clickhouse server automatically. 


#### Creating the database and table
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
