import argparse
import ConfigParser
import logging
import os
import pykafka
import _pytimeseries
import signal
import sys


class Proxy:

    def __init__(self, config_file):
        self.config_file = os.path.expanduser(config_file)

        self.config = None
        self._load_config()

        self.shutdown = 0
        signal.signal(signal.SIGTERM, self._stop_handler)
        signal.signal(signal.SIGINT, self._stop_handler)
        signal.signal(signal.SIGHUP, self._hup_handler)

        # initialize libtimeseries
        self.ts = None
        self._init_timeseries()

        self.kc = None
        self.consumer = None
        self._init_kafka()

    def _load_config(self):
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(open(self.config_file))
        # configure_logging MUST come before any calls to logging
        self._configure_logging()

    def _configure_logging(self):
        logging.basicConfig(level=self.config.get('logging', 'loglevel'),
                            format='%(asctime)s|TSK|%(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def _init_timeseries(self):
        logging.info("Initializing PyTimeseries")
        self.ts = _pytimeseries.Timeseries()
        for name in self.config.get('timeseries', 'backends').split(','):
            logging.info("Enabling timeseries backend '%s'" % name)
            be = self.ts.get_backend_by_name(name)
            if not be:
                logging.error("Could not enable TS backend %s" % name)
            opts = self.config.get('timeseries', name + '-opts')
            self.ts.enable_backend(be, opts)

    def _init_kafka(self):
        # connect to kafka
        if self.config.get('kafka', 'use_rdkafka') == 'True':
            use_rdkafka = True
        else:
            use_rdkafka = False
        self.kc = pykafka.KafkaClient(hosts=self.config.get('kafka', 'brokers'))
        # TODO: use balanced consumer
        self.consumer = self.kc.topics[self.config.get('kafka', 'topic')]\
            .get_simple_consumer(use_rdkafka=use_rdkafka,
                                 consumer_timeout_ms=10000)

    def _stop_handler(self, _signo, _stack_frame):
        logging.info("Caught signal, shutting down at next opportunity")
        self.shutdown += 1
        if self.shutdown > 3:
            logging.warn("Caught %d signals, shutting down NOW" % self.shutdown)
            sys.exit(0)

    def _hup_handler(self, _signo, _stack_frame):
        logging.info("caught HUP, reloading config")
        logging.error("NOT IMPLEMENTED")
        self.shutdown += 1

    def run(self):
        logging.info("TSK Proxy starting...")
        while True:
            # if we have been asked to shut down, do it now
            if self.shutdown:
                return

            for msg in self.consumer:
                if msg is not None:
                    self.handle_msg(msg.value)
                if self.shutdown:
                    break

    def handle_msg(self, msgbuf):
        pass


def main():
    parser = argparse.ArgumentParser(description="""
    Connects to a TimeSeries Kafka cluster and proxies metrics to other
    libtimeseries backends
    """)
    parser.add_argument('-c',  '--config-file',
                        nargs='?', required=True,
                        help='Configuration filename')

    opts = vars(parser.parse_args())

    tsk_proxy = Proxy(opts['config_file'])
    tsk_proxy.run()
