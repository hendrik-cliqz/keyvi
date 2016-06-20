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


PROCESSES = 2

# Queue we put the compilation tasks into
MERGER_QUEUE = multiprocessing.JoinableQueue(2 * PROCESSES)

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
            c = RPCClient('localhost', 6101)
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

class MergeScheduler(RPCServer):
    def __init__(self, index_dir="kv-index"):
        self.index_dir = index_dir
        self.index_file = os.path.join(index_dir, "index.toc")
        self.log = LOG
        self.log.info('MergeScheduler started')
        self.segments_in_merger = {}
        self.lock = gevent.lock.RLock()
        self.segments = []
        self.segments_marked_for_merge = []
        super(MergeScheduler, self).__init__(pack_params={'use_bin_type': True}, tcp_no_delay=True)

    def _find_merges(self):
        to_merge = []

        for segment in self.segments:
            if segment not in self.segments_marked_for_merge:
                self.log.info('add to merge list: {}'.format(segment))
                to_merge.append(segment)

        return to_merge

    def _write_toc(self):
        try:
            with self.lock:
                self.log.info("write new TOC")
                toc = json.dumps({"files": self.segments})
                fd = open("index.toc.new", "w")
                fd.write(toc)
                fd.close()
                move("index.toc.new", self.index_file)
        except:
            self.log.exception("failed to write toc")
            raise

    def register_new_segment(self, new_segment):
        # add new segment
        with self.lock:
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
            with self.lock:
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

if __name__ == '__main__':
    index_dir="kv-index"

    workers = {}

    for idx in range(0, PROCESSES):
        LOG.info("Start worker {}".format(idx))

        worker = multiprocessing.Process(target=_merge_worker,
                                    args=(idx, index_dir))
        worker.start()
        workers[idx] = worker

    m = MergeScheduler(index_dir)

    server = StreamServer(('127.0.0.1', 6101), m)
    server.start()
    while True: #self.alive:
        #self.notify()
        gevent.sleep(1.0)
        for idx, worker in workers.iteritems():
            if not worker.is_alive():
                print "respawn"
                worker.join()
                print "{}".format(worker.exitcode)
                new_worker = multiprocessing.Process(target=_merge_worker,
                                    args=(idx, index_dir))
                new_worker.start()
                workers[idx] = new_worker

