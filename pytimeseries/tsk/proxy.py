#
# Copyright (C) 2017 The Regents of the University of California.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

import argparse
import configparser
import confluent_kafka
import logging
import os
import _pytimeseries
import pytimeseries.utils
import signal
import struct
import sys
import time

HEADER_MAGIC_LEN = 8

TSKBATCH_VERSION = 0

STAT_METRIC_PFX = "systems.services.tsk"


class TskReader:

    def __init__(self, topic_prefix, channel, consumer_group, brokers,
                 partition=None, reset_offsets=False, commit_offsets=True):
        if sys.version_info[0] == 2:
            self.channel = channel
        else:
            self.channel = bytes(channel, 'ascii')
        # connect to kafka
        self.topic_name = ".".join([topic_prefix, channel])
        self.consumer_group = ".".join([consumer_group, self.topic_name])
        self.partition = partition
        conf = {
            'bootstrap.servers': brokers,
            'group.id': self.consumer_group,
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'heartbeat.interval.ms': 60000,
            'api.version.request': True,
            'enable.auto.commit': commit_offsets,
        }
        self.kc = confluent_kafka.Consumer(conf)

        if self.partition:
            topic_list = [confluent_kafka.TopicPartition(self.topic_name,
                                                         self.partition)]
            self.kc.assign(topic_list)
        else:
            self.kc.subscribe([self.topic_name])

        if reset_offsets:
            logging.info("Resetting commited offsets")
            raise NotImplementedError

    def close(self):
        return self.kc.close()

    def poll(self, time):
        return self.kc.poll(time)

    def handle_msg(self, msgbuf, msg_cb, kv_cb):
        try:
            msg_time, version, channel, offset = self._parse_header(msgbuf)
        except struct.error:
            raise RuntimeError("malformed Kafka message")

        msgbuflen = len(msgbuf)
        if version != TSKBATCH_VERSION:
            raise RuntimeError("Kafka message with version %d "
                "(expected %d)" % (version, TSKBATCH_VERSION))
        if channel != self.channel:
            raise RuntimeError("Kafka message with channel %s "
                "(expected %s)" % (channel, self.channel))

        if msg_cb != None:
            msg_cb(msg_time, version, channel, msgbuf, msgbuflen)

        while offset < msgbuflen:
            try:
                key, val, offset = self._parse_kv(msgbuf, offset)
            except struct.error:
                raise RuntimeEerror("Could not parse Kafka key/value")
            kv_cb(key, val)

    # Parse 2-byte network-order length and a bytestring of that length.
    # Return the bytestring and the new offset.
    @staticmethod
    def _parse_bytestr(msgbuf, offset):
        (blen,) = struct.unpack_from("!H", msgbuf, offset)
        offset += 2
        (bstr,) = struct.unpack_from("!%ds" % blen, msgbuf, offset)
        offset += blen
        return bstr, offset

    @staticmethod
    def _parse_header(msgbuf):
        # skip over the TSKBATCH magic
        offset = HEADER_MAGIC_LEN

        (version, time) = struct.unpack_from("!BL", msgbuf, offset)
        offset += 1 + 4

        channel, offset = TskReader._parse_bytestr(msgbuf, offset)

        return time, version, channel, offset

    def _parse_kv(self, msgbuf, offset):
        key, offset = TskReader._parse_bytestr(msgbuf, offset)

        (val, ) = struct.unpack_from("!Q", msgbuf, offset)
        offset += 8

        return key, val, offset


class Proxy:

    def __init__(self, config_file, reset_offsets,
                 partition=None, instance=None):
        self.config_file = os.path.expanduser(config_file)
        self.partition = partition
        self.instance = instance

        self.config = None
        self._load_config()

        # initialize libtimeseries
        self.ts = None
        self.kp = None
        self.current_time = None
        self._init_timeseries()

        self.tsk_reader = TskReader(
            self.config.get('kafka', 'topic_prefix'),
            self.config.get('kafka', 'channel'),
            self.config.get('kafka', 'consumer_group'),
            self.config.get('kafka', 'brokers'),
            self.partition,
            reset_offsets)

        # set up stats (needs kafka to be init first)
        self.stats_ts = None
        self.stats_kp = None
        self.stats_time = None
        self.stats_interval = 0
        self._init_stats()

        self.shutdown = 0
        signal.signal(signal.SIGTERM, self._stop_handler)
        signal.signal(signal.SIGINT, self._stop_handler)
        signal.signal(signal.SIGHUP, self._hup_handler)

    def _load_config(self):
        self.config = configparser.ConfigParser()
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
                raise ValueError("Could not enable TS backend %s" % name)
            opts = self.config.get('timeseries', name + '-opts')
            self.ts.enable_backend(be, opts)
        self.kp = self.ts.new_keypackage(reset=False, disable=True)

    def _stats_interval_now(self):
        return (time.time() // self.stats_interval) * self.stats_interval

    def _init_stats(self):
        self.stats_interval = int(self.config.get('stats', 'interval'))
        if not self.stats_interval:
            return
        logging.info("Initializing Stats")
        self.stats_ts = _pytimeseries.Timeseries()
        be_name = self.config.get('stats', 'ts_backend')
        be = self.stats_ts.get_backend_by_name(be_name)
        if not be:
            raise ValueError("Could not find TS backend %s" % be_name)
        opts = self.config.get('stats', 'ts_opts')
        if not self.stats_ts.enable_backend(be, opts):
            raise RuntimeError("Could not enable stats TS backend %s" % be_name)
        self.stats_kp = self.stats_ts.new_keypackage(reset=True, disable=False)
        self.stats_time = self._stats_interval_now()

    def _inc_stat(self, stat, value):
        if not self.stats_interval:
            return
        if self.instance is not None:
            stat = ".".join([
                pytimeseries.utils.graphite_safe_node(self.instance),
                stat
            ])
        key = ".".join([
            STAT_METRIC_PFX,
            pytimeseries.utils.graphite_safe_node(
                self.config.get('kafka', 'consumer_group')),
            pytimeseries.utils.graphite_safe_node(
                self.config.get('kafka', 'topic_prefix')),
            pytimeseries.utils.graphite_safe_node(
                self.config.get('kafka', 'channel')),
            stat
        ])
        idx = self.stats_kp.get_key(key)
        if idx is None:
            idx = self.stats_kp.add_key(key)
        old = self.stats_kp.get(idx)
        self.stats_kp.set(idx, old + value)

    def _maybe_flush_stats(self):
        if not self.stats_interval:
            return
        now = self._stats_interval_now()
        if now >= (self.stats_time + self.stats_interval):
            logging.debug("Flushing stats at %d" % self.stats_time)
            self.stats_kp.flush(self.stats_time)
            self.stats_time = now

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
            self._inc_stat("flush_cnt", 1)
            self._inc_stat("flushed_key_cnt", self.kp.enabled_size)
            self.kp.flush(self.current_time)
            # all keys are reset now
            assert(self.kp.enabled_size == 0)
            self.current_time = flush_time

    def _msg_cb(self, msg_time, version, channel, msgbuf, msgbuflen):
        self._maybe_flush(msg_time)
        self._inc_stat("messages_cnt", 1)
        self._inc_stat("messages_bytes", msgbuflen)

    def _kv_cb(self, key, val):
        idx = self.kp.get_key(key)
        if idx is None:
            idx = self.kp.add_key(key)
        else:
            self.kp.enable_key(idx)
        self.kp.set(idx, val)

    def run(self):
        logging.info("TSK Proxy starting...")
        while True:
            logging.info("Forcing a flush")
            self._maybe_flush()
            self._maybe_flush_stats()
            # if we have been asked to shut down, do it now
            if self.shutdown:
                self._maybe_flush()
                self.tsk_reader.close()
                logging.info("Shutdown complete")
                return
            # process some messages!
            msg = self.tsk_reader.poll(10000)
            eof_since_data = 0
            while msg is not None:
                if not msg.error():
                    try:
                        self.tsk_reader.handle_msg(msg.value(),
                            self._msg_cb, self._kv_cb)
                    except RuntimeError as e:
                        logging.error("Skipping " + str(e))
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
                msg = self.tsk_reader.poll(10000)
                self._maybe_flush_stats()


def main():
    parser = argparse.ArgumentParser(description="""
    Connects to a TimeSeries Kafka cluster and proxies metrics to other
    libtimeseries backends
    """)
    parser.add_argument('-c',  '--config-file',
                        required=True,
                        help='Configuration filename')

    parser.add_argument('-r',  '--reset-offsets',
                        action='store_true', required=False,
                        help='Reset committed offsets')

    parser.add_argument('-p',  '--partition',
                        required=False, default=None, type=int,
                        help='Partition to process (default: all)')

    parser.add_argument('-i',  '--instance',
                        required=False,
                        help='The name of this instance (default: unset)')

    opts = vars(parser.parse_args())

    proxy = Proxy(**opts)
    proxy.run()


if __name__ == '__main__':
    main()
