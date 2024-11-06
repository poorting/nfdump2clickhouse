#! /usr/bin/env python3
import os
import sys
import time
import pprint
import logging
from logging import handlers
import argparse
import configparser
import textwrap
import signal
import shutil
import tempfile
import subprocess

import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet as pq

from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
from watchdog.events import FileModifiedEvent
from multiprocessing.pool import Pool

import clickhouse_driver

from pathlib import Path
import re

program_name = os.path.basename(__file__)
VERSION = 0.1
logger = logging.getLogger(program_name)

sig_received = False

###############################################################################
# class Handler(PatternMatchingEventHandler):
class Handler(RegexMatchingEventHandler):

    def __init__(self, pool, ch_table='nfsen.flows', flowsrc='', use_fmt=False):
        super().__init__(regexes=[r'.*/nfcapd.\d{12}'],
                         ignore_directories=True)
        # super().__init__(regexes=['.*'],
        #                  ignore_directories=True)
        self.flowsrc = flowsrc
        self.ch_table = ch_table
        self.pool = pool
        self.use_fmt = use_fmt

    def completed_callback(self, result):
        logger.info(f"Completed: {result['src']} in {result['toCSV']+result['toParquet']+result['toCH']:.2f} seconds")
        logger.info(f"\t to CSV: {result['toCSV']:.2f}s, to Parquet: {result['toParquet']:.2f}s, CH ingest: {result['toCH']:.2f}s")

    def error_callback(self, error):
        logger.error(f"Error: {error}")

    def __convert(self, source_file):
        logger.info(f"Submitting {source_file} for conversion")
        self.pool.apply_async(convert, args=(source_file, self.ch_table, self.flowsrc, self.use_fmt),
                              callback=self.completed_callback,
                              error_callback=self.error_callback)

    def on_moved(self, event):
        logger.debug(f'Received moved event - {event.dest_path}')
        self.__convert(event.dest_path)

    def on_created(self, event):
        logger.debug(f'Received created event - {event.src_path}')
        self.__convert(event.src_path)

    # For some reasons the watcher fails after a length of time with
    # TypeError: expected str, bytes or os.PathLike object, not NoneType
    # In Handler.dispatch(event) (watchdog/events.py:476 in dispatch)
    # Which is this line:
    #  paths.append(os.fsdecode(event.dest_path))
    # Overriding the dispatch method to catch this exception and logging it...
    # So that at least the exception doesn't stop the watchdog
    def dispatch(self, event):
        pp = pprint.PrettyPrinter(indent=4)
        try:
            if not isinstance(event, FileModifiedEvent):
                logger.debug(event)
            super().dispatch(event)
        except TypeError as te:
            logger.error('TypeError on dispatch event')
            logger.error(te)
            logger.error(event)


###############################################################################
class ArgumentParser(argparse.ArgumentParser):

    def error(self, message):
        print('\n\033[1;33mError: {}\x1b[0m\n'.format(message))
        self.print_help(sys.stderr)
        # self.exit(2, '%s: error: %s\n' % (self.prog, message))
        self.exit(2)


###############################################################################
class CustomConsoleFormatter(logging.Formatter):
    """
        Log facility format
    """

    def format(self, record):
        # info = '\033[0;32m'
        info = ''
        warning = '\033[0;33m'
        error = '\033[1;33m'
        debug = '\033[1;34m'
        reset = "\x1b[0m"

        formatter = "%(levelname)s - %(message)s"
        if record.levelno == logging.INFO:
            log_fmt = info + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.WARNING:
            log_fmt = warning + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.ERROR:
            log_fmt = error + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.DEBUG:
            # formatter = '%(asctime)s %(levelname)s [%(filename)s.py:%(lineno)s/%(funcName)s] %(message)s'
            formatter = '%(levelname)s [%(filename)s:%(lineno)s/%(funcName)s] %(message)s'
            log_fmt = debug + formatter + reset
            self._style._fmt = log_fmt
        else:
            self._style._fmt = formatter

        return super().format(record)


###############################################################################
# Subroutines
def get_logger(logfile=None, debug=False):
    logger = logging.getLogger(program_name)

    # Create handlers
    console_handler = logging.StreamHandler()
    console_formatter = CustomConsoleFormatter()
    console_handler.setFormatter(console_formatter)

    if logfile:
        file_handler = logging.handlers.RotatingFileHandler(filename=logfile, backupCount=2, maxBytes=10**7)
        file_formatter = logging.Formatter('%(asctime)s  %(levelname)-5s %(filename)-10s %(lineno)d %(funcName)-20s %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    else:
        logger.addHandler(console_handler)

    logger.setLevel(logging.INFO)

    if debug:
        logger.setLevel(logging.DEBUG)

    return logger


# Subroutines
# ------------------------------------------------------------------------------
def parser_add_arguments():
    """
        Parse command line parameters
    """
    parser = ArgumentParser(
        prog=program_name,
        description=textwrap.dedent('''\
                        Watches a directory (and its subdirectories) for new nfcapd files appearing,
                         converts them to parquet and inserts into clickhouse.

                        Only files named 'nfcapd.YYYYMMDDHHMM' are picked up, thereby effectively ignoring files currently being generated by the nfdump tools.
                        '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("-b",
                        metavar="basedir",
                        help=textwrap.dedent('''\
                        Base directory to watch for nfdump files
                        '''),
                        action="store",
                        )

    parser.add_argument("-d",
                        metavar="database.table",
                        help=textwrap.dedent('''\
                        Database and table to use, specified as <db>.<table>.
                        Default is test.testflows if not specified
                        '''),
                        action="store",
                        )

    parser.add_argument("-j",
                        metavar="# of workers",
                        help=textwrap.dedent('''\
                        Number of workers (processes) to start for conversion.
                        Defaults to 1 if not specified.
                        '''),
                        action="store",
                        default=1,
                        )

    parser.add_argument("-f",
                        metavar='flowsrc',
                        help=textwrap.dedent('''\
                        Additional flowsrc name stored in the flowsrc column
                        '''),
                        action="store",
                        default=''
                        )

    parser.add_argument("-c",
                        metavar='config file',
                        help=textwrap.dedent('''\
                        load config from this file. 
                        If a config file is specified then all
                        other command line options are ignored
                        '''),
                        action="store",
                        default=''
                        )

    parser.add_argument("-l",
                        metavar='log file',
                        help=textwrap.dedent('''\
                        Log to the specified file instead
                        of logging to console.
                        '''),
                        action="store",
                        )

    parser.add_argument('-i', '--import',
                        type=Path,
                        help='nfcapd file(s) to convert and import, globbing supported',
                        nargs='+', required=False,
                        dest='imports')

    parser.add_argument('-u',
                        help=textwrap.dedent('''\
                        nfdump version newer than 1.7.4 use a different default csv output format
                        in which case a format string must be used for conversion to csv.
                        If you get errors along the lines of:
                        'ERROR - CSV parse error: Expected 48 columns, got 10'
                        try again with this argument supplied.
                        '''),
                        action="store_true")

    parser.add_argument("--debug",
                        help="show debug output",
                        action="store_true")

    parser.add_argument("-V", "--version",
                        help="print version and exit",
                        action="version",
                        version='%(prog)s (version {})'.format(VERSION))

    return parser


# ------------------------------------------------------------------------------
def create_db_and_table(client, db_and_table, ttl=90):
    db_create = f"CREATE DATABASE IF NOT EXISTS {db_and_table.split('.')[0]}"
    client.execute(db_create)

    tbl_create = f"""
        CREATE TABLE IF NOT EXISTS {db_and_table}
        (
            `ts` DateTime DEFAULT 0,
            `te` DateTime DEFAULT 0,
            `sa` String,
            `da` String,
            `sp` UInt16 DEFAULT 0,
            `dp` UInt16 DEFAULT 0,
            `pr` Nullable(String),
            `flg` LowCardinality(String),
            `ipkt` UInt64,
            `ibyt` UInt64,
            `smk` UInt8,
            `dmk` UInt8,
            `ra` LowCardinality(String),
            `in` UInt16 DEFAULT 0,
            `out` UInt16 DEFAULT 0,
            `sas` UInt32 DEFAULT 0,
            `das` UInt32 DEFAULT 0,
            `exid` UInt16 DEFAULT 0,
            `flowsrc` LowCardinality(String)
        )
        ENGINE = MergeTree
        PARTITION BY tuple()
        PRIMARY KEY (ts, te)
        ORDER BY (ts, te, sa, da)
        TTL te + toIntervalDay({ttl})
    """
    client.execute(tbl_create)


# ------------------------------------------------------------------------------
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    logger.debug("init_worker()")


# ------------------------------------------------------------------------------
def convert(src_file: str,
            ch_table='nfsen.flows',
            flowsrc='test',
            use_fmt=False,
            loglevel=logging.INFO,
            store_copy_dir: str=None):

    # Max size of chunk to read at a time
    block_size = 2 * 1024 * 1024

    # The default fields (order) present in the nfcapd files
    nf_fields = ['ts', 'te', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg',
                 'fwd', 'stos', 'ipkt', 'ibyt', 'opkt', 'obyt', 'in',
                 'out', 'sas', 'das', 'smk', 'dmk', 'dtos', 'dir',
                 'nh', 'nhb', 'svln', 'dvln', 'ismc', 'odmc', 'idmc',
                 'osmc', 'mpls1', 'mpls2', 'mpls3', 'mpls4', 'mpls5',
                 'mpls6', 'mpls7', 'mpls8', 'mpls9', 'mpls10', 'cl',
                 'sl', 'al', 'ra', 'eng', 'exid', 'tr']
    fmt_str = "csv:%ts,%te,%sa,%da,%sp,%dp,%pr,%flg,%ipkt,%ibyt,%smk,%dmk,%ra,%in,%out,%sas,%das,%exp"
    # The default fields that should be carried over to the parquet file
    # exid == exporter id
    parquet_fields = ['ts', 'te', 'sa', 'da', 'sp', 'dp', 'pr', 'flg',
                      'ipkt', 'ibyt', 'smk', 'dmk', 'ra', 'in', 'out', 'sas', 'das', 'exid']

    info_return = {
        'src': src_file,
    }

    drop_columns = [a for a in nf_fields if a not in parquet_fields]

    logger = logging.getLogger(program_name)
    logger.setLevel(loglevel)

    if not os.path.isfile(src_file):
        raise FileNotFoundError(src_file)

    logger.info(f'converting {src_file}')
    start = time.time()

    # Create a temp file for the intermediate CSV
    tmp_file, tmp_filename = tempfile.mkstemp()
    os.close(tmp_file)

    # Ensure timestamps stay in UTC rather than converted to local TZ
    new_env = dict(os.environ)
    new_env['TZ'] = 'UTC'

    try:
        with open(tmp_filename, 'a', encoding='utf-8') as f:
            if use_fmt:
                subprocess.run(['nfdump', '-r', src_file, '-o', fmt_str, '-q'], stdout=f, env=new_env)
            else:
                subprocess.run(['nfdump', '-r', src_file, '-o', 'csv', '-q'], stdout=f, env=new_env)
    except Exception as e:
        logger.error(f'Error reading {src_file} : {e}')
        return

    duration = time.time() - start
    info_return['toCSV'] = duration
    logger.debug(f"{src_file} to CSV in {duration:.2f}s")

    if store_copy_dir:
        logger.debug(f"storing copy of temp CSV file in {store_copy_dir}/{os.path.basename(src_file)}.csv")
        shutil.copyfile(tmp_filename, f"{store_copy_dir}/{os.path.basename(src_file)}.csv")

    # Create a temp file for the parquet file
    tmp_file, tmp_parquetfile = tempfile.mkstemp()
    os.close(tmp_file)

    start = time.time()
    pqwriter = None

    try:
        # Version 1.75-release of nfdump still outputs a header line when using -q option
        # This messes up the conversion to parquet since every column is then assumed to be a string
        # quick fix is to set skip_rows to 1 (rather than default 0)

        # read first line to determine if it is a header line
        skip_rows = 0
        with open(tmp_filename) as fp:
            header = fp.readline()
            if header.startswith('firstSeen'):
                skip_rows = 1
                logger.debug("csv exported by nfdump still contains header line, skipping")

        with pyarrow.csv.open_csv(input_file=tmp_filename,
                                  read_options=pyarrow.csv.ReadOptions(
                                      block_size=block_size,
                                      column_names=parquet_fields if use_fmt else nf_fields,
                                      skip_rows=skip_rows)
                                  ) as reader:
            chunk_nr = 0
            for next_chunk in reader:
                chunk_nr += 1
                if next_chunk is None:
                    break
                table = pa.Table.from_batches([next_chunk])

                if not use_fmt:
                    try:
                        table = table.drop(drop_columns)
                    except KeyError as ke:
                        logger.error(ke)

                table = table.append_column('flowsrc', [[flowsrc] * table.column('te').length()])

                if not pqwriter:
                    pqwriter = pq.ParquetWriter(tmp_parquetfile, table.schema)

                pqwriter.write_table(table)

    except pyarrow.lib.ArrowInvalid as e:
        logger.error(e)

    if pqwriter:
        pqwriter.close()

    duration = time.time() - start
    info_return['toParquet'] = duration
    logger.debug(f"{src_file} CSV to Parquet in {duration:.2f}s")
    if store_copy_dir:
        logger.debug(f"storing copy of temp Parquet file in {store_copy_dir}/{os.path.basename(src_file)}.parquet")
        shutil.copyfile(tmp_parquetfile, f"{store_copy_dir}/{os.path.basename(src_file)}.parquet")

    logger.debug(f"{src_file} Removing temporary file")
    # Remove temporary file
    os.remove(tmp_filename)

    # Import the parquet file into clickhouse
    start = time.time()
    try:
        with open(tmp_parquetfile, 'rb') as f:
            subprocess.run(['clickhouse-client', '--query', f"INSERT INTO {ch_table} FORMAT Parquet"], stdin=f)
    except Exception as e:
        print(f'Error : {e}')

    duration = time.time() - start
    info_return['toCH'] = duration
    logger.debug(f"Parquet ingest in CH in {duration:.2f}s")

    # Remove the temporary parquet file
    os.remove(tmp_parquetfile)

    return info_return


###############################################################################
def main():

    global sig_received
    def signal_handler(signum, frame):
        global sig_received
        sig_received = True
        signame = signal.Signals(signum).name
        logger.info(f'Signal {signame} received. Exiting gracefully.')

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    pp = pprint.PrettyPrinter(indent=4)

    logger = logging.getLogger(program_name)
    logfile = None

    parser = parser_add_arguments()
    args = parser.parse_args()

    if not args.c:
        if not args.b and not args.imports:
            parser.error("No basedir or imports provided. Provide either a basedir, imports or a configuration file")
            exit(1)

    watches = list()

    workers = int(args.j)

    if args.l:
        logfile = args.l

    flowsrc = 'test'
    if args.f:
        flowsrc = args.f

    db_tbl='test.testflows'
    if args.d:
        db_tbl = args.d

    if args.b and not os.path.isdir(args.b):
        logger.error(f"Directory to watch ({args.b}) not found or not a directory")
        exit(2)

    if args.b:
        watches.append({'watchdir': args.b,
                        'flowsrc': flowsrc,
                        'ch_table': db_tbl,
                        'ch_ttl': 90,
                        'use_fmt': args.u})

    # See if we have a config file
    if args.c and os.path.isfile(args.c):
        config = configparser.ConfigParser()
        config.read(args.c)
        try:
            logfile = config['DEFAULT']['logfile']
        except KeyError:
            None

        for section in config.sections():
            try:
                watchdir = config[section]['watchdir']
                ch_table = config[section]['ch_table']
                ch_ttl = config[section].get('ch_ttl', 90)
                workers = int(config[section].get('workers', 1))
                use_fmt = bool(config[section].get('use_fmt', False))

                if os.path.isdir(watchdir):
                    watches.append({'watchdir': watchdir,
                                    'ch_table': ch_table,
                                    'flowsrc': section,
                                    'ch_ttl': ch_ttl,
                                    'workers': workers,
                                    'use_fmt': use_fmt})
                else:
                    logger.error(f'watchdir in section [{section}] of {args.c} does not exist or is not a directory')

            except KeyError:
                logger.error(f'watchdir missing in section [{section}] of {args.c}')
        workers = 0
        for watch in watches:
            workers += watch['workers']

    logger = get_logger(logfile=logfile, debug=args.debug)

    if len(watches) == 0 and not args.imports:
        logger.error("No directories to watch or files to import, exiting.")
        exit(1)

    if args.imports and not args.f:
        logger.error("A flowsrc needs to be specified when importing files")
        exit(1)

    # Create database and table if they do not already exist
    client = clickhouse_driver.Client(host='localhost',  settings={'use_numpy': False})

    logger.info(f"Starting {workers} workers for conversion")

    pool = Pool(workers, init_worker)

    if args.imports:
        # imports specified on the command line
        logger.info(f"Specified files to import into '{db_tbl}' with flowsrc='{flowsrc}':")
        create_db_and_table(client, db_tbl)
        # '.*/nfcapd.\d{12}'
        import_files = [str(f) for f in args.imports if re.fullmatch(r'.*/nfcapd.\d{12}', str(f))]
        logger.info(import_files)
        logger.info(f"{len(import_files)} files to import")

        def completed_callback(result):
            if result:
                logger.info(
                    f"Completed: {result['src']} in {result['toCSV'] + result['toParquet'] + result['toCH']:.2f} seconds")
                logger.info(
                    f"\t to CSV: {result['toCSV']:.2f}s, to Parquet: {result['toParquet']:.2f}s, CH ingest: {result['toCH']:.2f}s")
            else:
                logger.info("completed_callback(None)")

            logger.info(f"{len(import_files)} left to ingest")
            if not sig_received:
                if len(import_files)>0:
                    imp = import_files.pop()
                    pool.apply_async(convert, args=(imp, db_tbl, flowsrc, args.u),
                                      callback=completed_callback,
                                      error_callback=error_callback)
            else:
                logger.info("Signal received, not submitting new files for ingest")

        def error_callback(error):
            logger.error(f"Error: {error}")

        init_sub_nr = workers if workers<len(import_files) else len(import_files)
        for i in range(0, init_sub_nr):
            f = import_files.pop()
            pool.apply_async(convert, args=(f, db_tbl, flowsrc, args.u),
                              callback=completed_callback,
                              error_callback=error_callback)
        try:
            while (not sig_received) and len(import_files) > 0:
                time.sleep(1)
        finally:
            pool.close()
            pool.join()

    else:
        # no imports specified on the command line, so setting up watches
        observer = Observer()
        for watch in watches:
            create_db_and_table(client, watch['ch_table'], watch['ch_ttl'])
            event_handler = Handler(pool,
                                    ch_table= watch['ch_table'],
                                    flowsrc=watch['flowsrc'],
                                    use_fmt=watch['use_fmt'])
            observer.schedule(event_handler, watch['watchdir'], recursive=True)
            logger.info(f"Starting watch on {watch['watchdir']}, with flowsr='{watch['flowsrc']}'")

        observer.start()
        try:
            while not sig_received:
                time.sleep(1)
        finally:
            observer.stop()
            observer.join()
            pool.close()
            pool.join()


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()
