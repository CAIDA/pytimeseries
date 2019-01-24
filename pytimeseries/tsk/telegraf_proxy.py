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

# TODO: tune this
TELEGRAF_BATCH_SIZE = 5000


class TelegrafProxy:

    def __init__(self, config_file):
        self.config_file = os.path.expanduser(config_file)

        self.config = None
        self._load_config()

        # buffer of parsed series ready to be sent to kafka
        self.series = []
        # current timestamp we're processing
        self.current_time = None
        # number of series we've proxied in this timestamp
        self.series_cnt = 0

        self.tsk_consumer = None
        self.tsk_channel = None
        self._init_tsk_kafka()

        self.telegraf_topic = None
        self.telegraf_producer = None
        self._init_telegraf_kafka()

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
        self.tsk_channel = self.config.get('tsk', 'channel')
        topic_name = "%s.%s" % (self.config.get('tsk', 'topic_prefix'),
                                self.tsk_channel)
        consumer_group = "%s.%s" % \
                         (self.config.get('tsk', 'consumer_group'),
                          topic_name)
        conf = {
            'bootstrap.servers': self.config.get('tsk', 'brokers'),
            'group.id': consumer_group,
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'heartbeat.interval.ms': 60000,
            'api.version.request': True,
        }
        self.tsk_consumer = confluent_kafka.Consumer(**conf)
        self.tsk_consumer.subscribe([topic_name])

    def _init_telegraf_kafka(self):
        # connect to kafka
        self.telegraf_topic = self.config.get('telegraf', 'topic')
        conf = {
            'bootstrap.servers': self.config.get('telegraf', 'brokers'),
        }
        self.telegraf_producer = confluent_kafka.Producer(conf)
        logging.info("Initialized telegraf kafka producer (topic: %s)" % self.telegraf_topic)

    def _stop_handler(self, _signo, _stack_frame):
        logging.info("Caught signal, shutting down at next opportunity")
        self.shutdown += 1
        if self.shutdown > 3:
            logging.warn("Caught %d signals, shutting down NOW" % self.shutdown)
            sys.exit(0)

    def _maybe_tx(self, force=True):
        # if we have enough metrics in our buffer to fill an output message
        # then send that now and remove them from the buffer
        while len(self.series) >= TELEGRAF_BATCH_SIZE or (len(self.series) and force):
            this_batch = self.series[:TELEGRAF_BATCH_SIZE]
            self.series = self.series[TELEGRAF_BATCH_SIZE:]
            self.telegraf_producer.poll(0)
            self.telegraf_producer.produce(self.telegraf_topic, "\n".join(this_batch))

        if force:
            assert len(self.series) == 0

    def _handle_msg(self, msgbuf):
        try:
            msg_time, version, channel, offset = self._parse_header(msgbuf)
        except struct.error:
            logging.error("Malformed message received from Kafka, skipping")
            return

        if msg_time != self.current_time:
            if self.current_time is not None:
                logging.info("Proxied %d series at time %d" % (self.series_cnt, self.current_time))
            self.series_cnt = 0
            self.current_time = msg_time

        msgbuflen = len(msgbuf)
        if version != TSKBATCH_VERSION:
            logging.error("Message with unknown version %d, expecting %d" %
                          (version, TSKBATCH_VERSION))
            return
        if channel != self.tsk_channel:
            logging.error("Message with unknown channel %s, expecting %s" %
                          (channel, self.tsk_channel))
            return

        self._maybe_tx(force=False)

        while offset < msgbuflen:
            try:
                offset = self._parse_kv(msg_time, msgbuf, offset)
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

    def _parse_kv(self, time, msgbuf, offset):
        (keylen,) = struct.unpack_from("!H", msgbuf, offset)
        offset += 2
        (key, val, ) = struct.unpack_from("!%dsQ" % keylen, msgbuf, offset)
        offset += keylen + 8

        # TODO: support coalescing of fields (they will often be adjacent in the TSK message)
        tg_key, tg_field = self._parse_key(key)
        self.series.append("%s %s=%di %d" % (tg_key, tg_field, val, time * 1e9))
        self.series_cnt += 1

        return offset

    def _parse_key(self, key):
        measurement = None
        tagset = []
        field = None
        # TODO: better (config-based) parsing of graphite metrics?
        nodes = key.split(".")
        assert key.startswith("active.ping-slash24.")  # nodes[0],nodes[1] are constant

        # basically walk down the hierarchy to figure out tags/fields
        # if parsing fails, leave field unset to raise an exception
        if nodes[2] == "asn":
            # active.ping-slash24.asn.1968.probers.team-1.caida-sdsc.prober-3.uncertain_slash24_cnt
            measurement = "routing"

            tagset.append(("asn", nodes[3]))
            tagset.append(("team", nodes[5]))  # nodes[6] is "caida-sdsc"
            tagset.append(("prober", nodes[7]))

            if nodes[8] == "blocks":
                block = nodes[9].replace("__PFX_", "").replace("-", ".").replace("_", "/")
                tagset.append(("block", block))
                field = nodes[10]
            elif nodes[8].endswith("_slash24_cnt"):
                leaf = nodes[8].split("_")
                tagset.append(("state", leaf[0]))
                field = "_".join(leaf[1:])

        elif nodes[2] == "geo":
            # active.ping-slash24.geo.netacuity.NA.probers.team-1.caida-sdsc.prober-1.uncertain_slash24_cnt
            measurement = "geo"

            tagset.append(("geo_db", nodes[3]))
            tagset.append(("continent", nodes[4]))
            remain = []
            if nodes[5] == "probers":
                # continent
                remain = nodes[5:]
            elif nodes[6] == "probers":
                # country
                tagset.append(("country", nodes[5]))
                remain = nodes[6:]
            elif nodes[7] == "probers":
                # region
                tagset.append(("country", nodes[5]))
                tagset.append(("region", nodes[6]))
                remain = nodes[7:]
            elif nodes[8] == "probers":
                # county
                tagset.append(("country", nodes[5]))
                tagset.append(("region", nodes[6]))
                tagset.append(("county", nodes[7]))
                remain = nodes[8:]

            if remain[0] == "probers":  # just to be sure...
                tagset.append(("team", remain[1]))
                tagset.append(("prober", remain[3]))
                leaf = remain[4].split("_")
                tagset.append(("state", leaf[0]))
                field = "_".join(leaf[1:])

        elif nodes[2] == "probers":
            measurement = "overall"

            tagset.append(("team", nodes[3]))  # nodes[4] is "caida-sdsc"
            tagset.append(("prober", nodes[5]))
            if nodes[6] == "meta":
                field = "_".join(nodes[7:])
            elif nodes[6] == "probing":
                tagset.append(("probing_mode", nodes[7]))
                field = nodes[8]
            elif nodes[6] == "slash24_cnt":
                field = nodes[6]
            elif nodes[6] == "states":
                leaf = nodes[7].split("_")
                tagset.append(("state", leaf[0]))
                field = "_".join(leaf[1:])

        if measurement is None or field is None:
            raise ValueError(key)

        key = "%s,%s" % (measurement, ",".join(["=".join(tag) for tag in tagset]))
        return key, field

    def run(self):
        logging.info("TSK Proxy starting...")
        while True:
            logging.info("Forcing a flush")
            self._maybe_tx()
            # if we have been asked to shut down, do it now
            if self.shutdown:
                self._maybe_tx()
                self.tsk_consumer.close()
                self.telegraf_producer.flush()
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
