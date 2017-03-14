import argparse
import ConfigParser
import logging
import os
import pykafka
import _pytimeseries
import signal
import struct
import sys

HEADER_MAGIC_LEN = 8

TSKBATCH_VERSION = 0


class Proxy:

    def __init__(self, config_file, reset_offsets):
        self.config_file = os.path.expanduser(config_file)

        self.config = None
        self._load_config()

        self.shutdown = 0
        signal.signal(signal.SIGTERM, self._stop_handler)
        signal.signal(signal.SIGINT, self._stop_handler)
        signal.signal(signal.SIGHUP, self._hup_handler)

        # initialize libtimeseries
        self.ts = None
        self.current_time = None
        self._init_timeseries()

        self.kc = None
        self.consumer = None
        self._init_kafka(reset_offsets)

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
        self.kp = self.ts.new_keypackage(reset=False, disable=True)

    def _init_kafka(self, reset_offsets=False):
        # connect to kafka
        if self.config.get('kafka', 'use_rdkafka') == 'True':
            use_rdkafka = True
        else:
            use_rdkafka = False
        self.kc = pykafka.KafkaClient(hosts=self.config.get('kafka', 'brokers'))
        topic_name = "%s.%s" % (self.config.get('kafka', 'topic_prefix'),
                                self.config.get('kafka', 'channel'))
        # it seems the managed consumer doesn't do well with multiple topics on
        # a single consumer group, so we namespace our group to include the
        # topic name
        consumer_group = "%s.%s" % (self.config.get('kafka', 'consumer_group'),
                                    topic_name)
        self.consumer = self.kc.topics[topic_name]\
            .get_balanced_consumer(consumer_group,
                                   managed=True, use_rdkafka=use_rdkafka,
                                   auto_commit_enable=True,
                                   auto_offset_reset=pykafka.common.OffsetType.EARLIEST,
                                   consumer_timeout_ms=10000)

        if reset_offsets:
            logging.info("Resetting commited offsets")
            self.consumer.reset_offsets()

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

    def _maybe_flush(self, time=None):
        # if this is not our first message, and this time is different than
        # the current time, we need to dump the KP
        if not self.current_time:
            self.current_time = time
        elif not time or (time != self.current_time):
            # now flush the key package
            logging.debug("Flushing KP at %d with %d keys enabled (%d total)" %
                          (self.current_time, self.kp.enabled_size,
                           self.kp.size))
            self.kp.flush(self.current_time)
            # all keys are reset now
            assert(self.kp.enabled_size == 0)
            self.current_time = time

    def _handle_msg(self, msgbuf):
        time, version, channel, offset = self._parse_header(msgbuf)
        if version != TSKBATCH_VERSION:
            logging.error("Message with unknown version %d, expecting %d" %
                          (version, TSKBATCH_VERSION))
            return
        if channel != self.config.get('kafka', 'channel'):
            logging.error("Message with unknown channel %s, expecting %s" %
                          (channel, self.config.get('kafka', 'channel')))
            return

        self._maybe_flush(time)

        msgbuflen = len(msgbuf)
        while offset < msgbuflen:
            offset = self._parse_kv(msgbuf, offset)

    @staticmethod
    def _parse_header(msgbuf):
        # skip over the TSKBATCH magic
        offset = HEADER_MAGIC_LEN

        (version, time, chanlen) = \
            struct.unpack_from("!BLH", msgbuf, offset)
        offset += 1 + 4 + 2

        (channel,) = struct.unpack_from("%ds" % chanlen, msgbuf, offset)
        offset += chanlen

        return time, version, channel, offset

    def _parse_kv(self, msgbuf, offset):
        (keylen,) = struct.unpack_from("!H", msgbuf, offset)
        offset += 2
        (key, val, ) = struct.unpack_from("!%dsQ" % keylen, msgbuf, offset)
        offset += keylen + 8

        idx = self.kp.get_key(key)
        if idx is None:
            idx = self.kp.add_key(key)
        else:
            self.kp.enable_key(idx)
        self.kp.set(idx, val)

        return offset

    def run(self):
        logging.info("TSK Proxy starting...")
        while True:
            # if we have been asked to shut down, do it now
            if self.shutdown:
                self._maybe_flush()
                return
            # process some messages!
            for msg in self.consumer:
                if msg is not None:
                    # will retry until successful...
                    self._handle_msg(buffer(msg.value))
                if self.shutdown:
                    break

def main():
    parser = argparse.ArgumentParser(description="""
    Connects to a TimeSeries Kafka cluster and proxies metrics to other
    libtimeseries backends
    """)
    parser.add_argument('-c',  '--config-file',
                        nargs='?', required=True,
                        help='Configuration filename')

    parser.add_argument('-r',  '--reset-offsets',
                        action='store_true', required=False,
                        help='Reset committed offsets')

    opts = vars(parser.parse_args())

    tsk_proxy = Proxy(**opts)
    tsk_proxy.run()
