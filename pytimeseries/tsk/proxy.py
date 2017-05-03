import argparse
import ConfigParser
import confluent_kafka
import logging
import os
import _pytimeseries
import signal
import struct
import sys

HEADER_MAGIC_LEN = 8

TSKBATCH_VERSION = 0


class Proxy:

    def __init__(self, config_file, reset_offsets, partition=None):
        self.config_file = os.path.expanduser(config_file)
        self.partition = partition

        self.config = None
        self._load_config()

        # initialize libtimeseries
        self.ts = None
        self.current_time = None
        self._init_timeseries()

        self.kc = None
        self.consumer = None
        self._init_kafka(reset_offsets)

        self.shutdown = 0
        signal.signal(signal.SIGTERM, self._stop_handler)
        signal.signal(signal.SIGINT, self._stop_handler)
        signal.signal(signal.SIGHUP, self._hup_handler)

    def _load_config(self):
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(open(self.config_file))
        # configure_logging MUST come before any calls to logging
        self._configure_logging()

    def _configure_logging(self):
        part_name = 'ALL'
        if self.partition is not None:
            part_name = str(self.partition)
        logging.basicConfig(level=self.config.get('logging', 'loglevel'),
                            format='%(asctime)s|TSK|PART-' + part_name
                                   + '|%(levelname)s: %(message)s',
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
        topic_name = "%s.%s" % (self.config.get('kafka', 'topic_prefix'),
                                self.config.get('kafka', 'channel'))
        consumer_group = "%s.%s" % (self.config.get('kafka', 'consumer_group'),
                                    topic_name)
        conf = {
            'bootstrap.servers': self.config.get('kafka', 'brokers'),
            'group.id': consumer_group,
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'heartbeat.interval.ms': 60000,
            'api.version.request': True,
        }
        self.kc = confluent_kafka.Consumer(**conf)

        if self.partition:
            topic_list = [confluent_kafka.TopicPartition(topic_name,
                                                         self.partition)]
            self.kc.assign(topic_list)
        else:
            self.kc.subscribe([topic_name])

        if reset_offsets:
            logging.info("Resetting commited offsets")
            raise NotImplementedError

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

    def _maybe_flush(self, flush_time=None):
        # if this is not our first message, and this time is different than
        # the current time, we need to dump the KP
        if not self.current_time:
            self.current_time = flush_time
        elif not flush_time or (flush_time != self.current_time):
            # now flush the key package
            logging.debug("Flushing KP at %d with %d keys enabled (%d total)" %
                          (self.current_time, self.kp.enabled_size,
                           self.kp.size))
            self.kp.flush(self.current_time)
            # all keys are reset now
            assert(self.kp.enabled_size == 0)
            self.current_time = flush_time

    def _handle_msg(self, msgbuf):
        msg_time, version, channel, offset = self._parse_header(msgbuf)
        if version != TSKBATCH_VERSION:
            logging.error("Message with unknown version %d, expecting %d" %
                          (version, TSKBATCH_VERSION))
            return
        if channel != self.config.get('kafka', 'channel'):
            logging.error("Message with unknown channel %s, expecting %s" %
                          (channel, self.config.get('kafka', 'channel')))
            return

        self._maybe_flush(msg_time)

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
            logging.info("Forcing a flush")
            self._maybe_flush()
            # if we have been asked to shut down, do it now
            if self.shutdown:
                self._maybe_flush()
                self.kc.close()
                logging.info("Shutdown complete")
                return
            # process some messages!
            msg = self.kc.poll(10000)
            while msg is not None:
                if not msg.error():
                    self._handle_msg(buffer(msg.value()))
                elif msg.error().code() == \
                        confluent_kafka.KafkaError._PARTITION_EOF:
                    # no new messages, force a flush
                    break
                else:
                    logging.error("Unhandled Kafka error, shutting down")
                    logging.error(msg.error())
                    self.shutdown = True
                if self.shutdown:
                    break
                msg = self.kc.poll(10000)


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

    parser.add_argument('-p',  '--partition',
                        nargs='?', required=False, default=None, type=int,
                        help='Partition to process (default: all)')

    opts = vars(parser.parse_args())

    proxy = Proxy(**opts)
    proxy.run()
