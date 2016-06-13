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

MERGER_PROCESSES = 2

# Queue we put the compilation tasks into
MERGER_QUEUE = multiprocessing.JoinableQueue(2 * MERGER_PROCESSES)

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
            if self._writer.compiler is not None:
                self.commit()

    def commit(self):
        compiler = self._writer.compiler
        self._writer.compiler = None
        compiler.Compile()
        filename = os.path.join(self._writer.index_dir, "{}-{}.kv".format(datetime.now(), os.getpid()))
        compiler.WriteToFile(filename + ".part")
        os.rename(filename+".part", filename)
        #c = RPCClient('localhost', 6101)
        #c.call('register_new_segment', filename)
        self._writer._register_new_segment(filename)


class IndexWriter(RPCServer):
    def __init__(self, index_dir="kv-index"):
        self.index_dir = index_dir
        self.log = LOG
        self.index_file = os.path.join(index_dir, "index.toc")
        LOG.info('Writer/Merger started')
        self.compiler = None
        self.segments_in_merger = {}
        self.merger_lock = gevent.lock.RLock()
        self.segments = []
        self.segments_marked_for_merge = []

        interval = 10
        self._finalizer = IndexFinalizer(self, interval)
        self._finalizer.start()
        super(IndexWriter, self).__init__(pack_params={'use_bin_type': True}, tcp_no_delay=True)

    def _find_merges(self):
        to_merge = []

        for segment in self.segments:
            if segment not in self.segments_marked_for_merge:
                self.log.info('add to merge list: {}'.format(segment))
                to_merge.append(segment)

        return to_merge

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

        to_merge = self._find_merges()
        if len(to_merge) > 1:
            self.segments_marked_for_merge.extend(to_merge)
            self.log.info("Put merge list into queue")
            MERGER_QUEUE.put(to_merge)

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


        if self.compiler is None:
            self.compiler=pykeyvi.JsonDictionaryCompiler()
        self.compiler.Add(key, value)

        return

    def set_many(self, key_value_pairs):pass

    def set_many_bulk(self, key_value_pairs):pass

    def commit(self):
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

