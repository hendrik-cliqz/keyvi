# -*- coding: utf-8 -*-

import os
import logging
from logging.handlers import RotatingFileHandler
import glob
import pykeyvi
from datetime import datetime
import multiprocessing
import gevent
import gevent.lock
import json
from shutil import move
from mprpc import RPCServer
from mprpc import RPCClient
from gevent.server import StreamServer

def _create_logger():
    def setup_logger(log):
        log.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s(%(process)s) - %(levelname)s - %(message)s')
        file_name = os.path.join('rpc-server.log')
        h = RotatingFileHandler(file_name, maxBytes=10 * 1024 * 1024,
                                backupCount=5)
        h.setFormatter(formatter)
        log.addHandler(h)
    log = logging.getLogger('kv_logger')
    setup_logger(log)

    return log

LOG = _create_logger()


MERGER_PROCESSES = 2

# Queue we put the compilation tasks into
MERGER_QUEUE = multiprocessing.JoinableQueue()

# blocking queue leads to a deadlock, therefore checking ourselves
MERGER_QUEUE_MAX_SIZE = MERGER_PROCESSES * 3

SEGMENT_WRITE_TRIGGER = 10000
SEGMENT_WRITE_INTERVAL = 3

def _merge_worker(idx, index_dir="kv-index"):
    LOG.info("Merge Worker {} started".format(idx))

    while True:
        job = MERGER_QUEUE.get()
        try:
            new_segment = _merge(job, index_dir)
        except:
            LOG.exception("Merge failed, worker {}".format(idx))
            raise

        LOG.info("call finalize_merge, worker {}".format(idx))
        try:
            c = RPCClient('localhost', 6100)
            c.call('finalize_merge', job, new_segment)
        except:
            LOG.exception('failed to call finalize')

        LOG.info("ready for next merge, worker {}".format(idx))

def _merge(list_to_merge, index_dir):

    LOG.info("start merge")
    merger = pykeyvi.JsonDictionaryMerger()
    for f in list_to_merge:
        if type(f) == unicode:
            f = f.encode("utf-8")

        LOG.info('add to merger: {}'.format(f))
        merger.Add(f)

    filename = os.path.join(index_dir, "{}-{}.kv".format(datetime.now(), os.getpid()))
    merger.Merge(filename)
    LOG.info("finished merge")

    return filename


class IndexFinalizer(gevent.Greenlet):
    def __init__(self, writer, interval=1):
        self._writer = writer
        self._delay = interval
        super(IndexFinalizer, self).__init__()

    def _run(self):
        while True:
            gevent.sleep(self._delay)
            self.commit()
            self._writer.find_merges()

    def commit(self, async=False):
        if self._writer.compiler is None:
            return

        compiler = self._writer.compiler
        self._writer.compiler = None

        # check it again, to be sure
        if compiler is None:
            return
        LOG.info("creating segment")

        def run_compile(compiler):
            compiler.Compile()
            filename = os.path.join(self._writer.index_dir, "{}-{}.kv".format(datetime.now(), os.getpid()))
            compiler.WriteToFile(filename + ".part")
            os.rename(filename+".part", filename)
            #c = RPCClient('localhost', 6101)
            #c.call('register_new_segment', filename)
            self._writer._register_new_segment(filename)
        run_compile(compiler)


class IndexWriter(RPCServer):
    def __init__(self, index_dir="kv-index"):
        self.index_dir = index_dir
        self.log = LOG
        self.index_file = os.path.join(index_dir, "index.toc")
        LOG.info('Writer/Merger started')
        self.segments_in_merger = {}
        self.merger_lock = gevent.lock.RLock()
        self.segments = []
        self.segments_marked_for_merge = []
        self._load_index_file()
        self.write_counter = 0

        self._finalizer = IndexFinalizer(self, SEGMENT_WRITE_INTERVAL)
        self._finalizer.start()

        self.compiler = None
        super(IndexWriter, self).__init__(pack_params={'use_bin_type': True}, tcp_no_delay=True)

    def _load_index_file(self):

        if not os.path.exists(self.index_file):
            return
        toc = '\n'.join(open(self.index_file).readlines())
        try:
            toc = json.loads(toc)
            self.segments = toc.get('files', [])
            self.log.info("loaded index")

        except Exception, e:
            self.log.exception("failed to load index")
            raise

    def _init_lazy_compiler(self):
        if not self.compiler:
            self.compiler = pykeyvi.JsonDictionaryCompiler(1024*1024*10)
            self.write_counter = 0

    def _find_merges(self):
        if (len(self.segments) - len(self.segments_marked_for_merge)) < 2 or MERGER_QUEUE.qsize() >= MERGER_QUEUE_MAX_SIZE:
            #LOG.info("skip merge, to many items in queue or to few segments")
            return []

        to_merge = []

        for segment in self.segments:
            if segment not in self.segments_marked_for_merge:
                to_merge.append(segment)

        if len(to_merge) > 1:
            with self.merger_lock:
                self.segments_marked_for_merge.extend(to_merge)
            self.log.info("Start merge of {} segments".format(len(to_merge)))
            MERGER_QUEUE.put(to_merge)

    def _write_toc(self):
        try:
            with self.merger_lock:
                self.log.info("write new TOC")
                toc = json.dumps({"files": self.segments})
                fd = open("index.toc.new", "w")
                fd.write(toc)
                fd.close()
                move("index.toc.new", self.index_file)
        except:
            self.log.exception("failed to write toc")
            raise

    def _register_new_segment(self, new_segment):
        # add new segment
        with self.merger_lock:
            self.segments.append(new_segment)

        # re-write toc to make new segment available
        self._write_toc()
        return

    def ping(self, x):
        return "HELLO " + x

    def finalize_merge(self, job, new_segment):
        self.log.info("finalize_merge called")
        try:
            self.log.info("finalize merge, put it into the index")
            with self.merger_lock:
                new_segments = [item for item in self.segments if item not in job]

                new_segments.append(new_segment)
                self.segments = new_segments

                # remove from marker list
                for item in job:
                    self.segments_marked_for_merge.remove(item)

            self._write_toc()

            # delete old files
            for f in job:
                os.remove(f)

        except:
            self.log.exception("Failed to finalize index")
            raise

        return "SUCCESS"


    def set(self, key, value):
        if key is None:
            return
        if type(key) == unicode:
            key = key.encode("utf-8")
        if type(value) == unicode:
            value = value.encode("utf-8")

        self._init_lazy_compiler()
        self.compiler.Add(key, value)
        self.write_counter += 1

        if self.write_counter >= SEGMENT_WRITE_TRIGGER:
            gevent.spawn(self._finalizer.commit)

        return

    def set_many(self, key_value_pairs):
        for key, value in key_value_pairs:
            self.set(key, value)

    def set_many_bulk(self, client_token, key_value_pairs, optimistic=False):
        for key, value in key_value_pairs:
            if type(key) == unicode:
                key = key.encode("utf-8")
            if type(value) == unicode:
                value = value.encode("utf-8")

            # todo: use bulk compiler
            if self.compiler is None:
                self.compiler=self._init_lazy_compiler()
            self.compiler.Add(key, value)

    def commit(self, async=True):
        self._finalizer.commit()


if __name__ == '__main__':
    index_dir = "kv-index"

    m = IndexWriter(index_dir)
    merge_workers = {}

    for idx in range(0, MERGER_PROCESSES):
        LOG.info("Start merge worker {}".format(idx))

        worker = multiprocessing.Process(target=_merge_worker,
                                    args=(idx, index_dir))
        worker.start()
        merge_workers[idx] = worker

    server = StreamServer(('127.0.0.1', 6100), m)
    server.start()
    while True: #self.alive:
        #self.notify()
        gevent.sleep(1.0)
        for idx, worker in merge_workers.iteritems():
            if not worker.is_alive():
                print "respawn"
                worker.join()
                print "{}".format(worker.exitcode)
                new_worker = multiprocessing.Process(target=_merge_worker,
                                    args=(idx, index_dir))
                new_worker.start()
                merge_workers[idx] = new_worker

