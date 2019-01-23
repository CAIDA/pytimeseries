import argparse
import ConfigParser
import confluent_kafka
import logging
import os
import signal
import struct
import sys

HEADER_MAGIC_LEN = 8

TSKBATCH_VERSION = 0


class TelegrafProxy:

    def __init__(self, config_file):
        self.config_file = os.path.expanduser(config_file)

        self.config = None
        self._load_config()

        self.tsk_consumer = None
        self._init_tsk_kafka()

        self.shutdown = 0
        signal.signal(signal.SIGTERM, self._stop_handler)
        signal.signal(signal.SIGINT, self._stop_handler)

    def _load_config(self):
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(open(self.config_file))
        # configure_logging MUST come before any calls to logging
        self._configure_logging()

    def _configure_logging(self):
        logging.basicConfig(level=self.config.get('logging', 'loglevel'),
                            format='%(asctime)s|TSK|%(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def _init_tsk_kafka(self):
        # connect to kafka
        topic_name = "%s.%s" % (self.config.get('kafka', 'topic_prefix'),
                                     self.config.get('kafka', 'channel'))
        consumer_group = "%s.%s" % \
                         (self.config.get('kafka', 'consumer_group'),
                          topic_name)
        conf = {
            'bootstrap.servers': self.config.get('kafka', 'brokers'),
            'group.id': consumer_group,
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'heartbeat.interval.ms': 60000,
            'api.version.request': True,
        }
        self.tsk_consumer = confluent_kafka.Consumer(**conf)
        self.tsk_consumer.subscribe([topic_name])

    def _stop_handler(self, _signo, _stack_frame):
        logging.info("Caught signal, shutting down at next opportunity")
        self.shutdown += 1
        if self.shutdown > 3:
            logging.warn("Caught %d signals, shutting down NOW" % self.shutdown)
            sys.exit(0)

    def _maybe_tx(self):
        # if we have enough metrics in our buffer to fill an output message
        # then send that now and remove them from the buffer
        pass

    def _handle_msg(self, msgbuf):
        try:
            msg_time, version, channel, offset = self._parse_header(msgbuf)
        except struct.error:
            logging.error("Malformed message received from Kafka, skipping")
            return

        msgbuflen = len(msgbuf)
        if version != TSKBATCH_VERSION:
            logging.error("Message with unknown version %d, expecting %d" %
                          (version, TSKBATCH_VERSION))
            return
        if channel != self.config.get('kafka', 'channel'):
            logging.error("Message with unknown channel %s, expecting %s" %
                          (channel, self.config.get('kafka', 'channel')))
            return

        self._maybe_tx()

        while offset < msgbuflen:
            try:
                offset = self._parse_kv(msgbuf, offset)
            except struct.error:
                logging.error("Could not parse key/value")
                return

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

        # TODO parse graphite path into influx format
        # TODO add metric to buffer

        return offset

    def run(self):
        logging.info("TSK Proxy starting...")
        while True:
            logging.info("Forcing a flush")
            self._maybe_tx()
            # if we have been asked to shut down, do it now
            if self.shutdown:
                self._maybe_tx()
                self.tsk_consumer.close()
                logging.info("Shutdown complete")
                return
            # process some messages!
            msg = self.tsk_consumer.poll(10000)
            eof_since_data = 0
            while msg is not None:
                if not msg.error():
                    self._handle_msg(buffer(msg.value()))
                    eof_since_data = 0
                elif msg.error().code() == \
                        confluent_kafka.KafkaError._PARTITION_EOF:
                    # no new messages, wait a bit and then force a flush
                    eof_since_data += 1
                    if eof_since_data >= 10:
                        break
                else:
                    logging.error("Unhandled Kafka error, shutting down")
                    logging.error(msg.error())
                    self.shutdown = True
                if self.shutdown:
                    break
                msg = self.tsk_consumer.poll(10000)


def main():
    parser = argparse.ArgumentParser(description="""
    Connects to a TimeSeries Kafka cluster and proxies metrics to a Kafka topic
    in the InfluxDB Line Protocol format that Telegraf understands
    """)
    parser.add_argument('-c',  '--config-file',
                        nargs='?', required=True,
                        help='Configuration filename')

    opts = vars(parser.parse_args())

    proxy = TelegrafProxy(**opts)
    proxy.run()
