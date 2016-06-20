from __future__ import unicode_literals

import multiprocessing

import gunicorn.app.base
import gunicorn.util

from gunicorn.six import iteritems
import gevent
from gevent.server import StreamServer

import core.index_reader
import core.index_writer


def number_of_workers():
    return (multiprocessing.cpu_count() * 2) + 1

class StandaloneApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, options=None):
        self.options = options or {}

        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        self.mprpc = core.index_reader.IndexReader


def start_reader():
    options = {
        'bind': '%s:%s' % ('0.0.0.0', '9100'),
        'workers': number_of_workers(),
        'worker_class': 'gunicorn_mprpc.mprpc_gevent_worker.MPRPCGeventWorker'

    }

    StandaloneApplication(options).run()

if __name__ == '__main__':
    index_dir = "kv-index"
    merge_processes = 2


    #StandaloneApplication(options).run()

    reader = multiprocessing.Process(target=start_reader)
    reader.start()

    merge_workers = {}

    for idx in range(0, merge_processes):
        #LOG.info("Start merge worker {}".format(idx))

        worker = multiprocessing.Process(target=core.index_writer._merge_worker,
                                    args=(idx, index_dir))
        worker.start()
        merge_workers[idx] = worker


    m = core.index_writer.IndexWriter(index_dir)

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
                new_worker = multiprocessing.Process(target=core.index_writer._merge_worker,
                                    args=(idx, index_dir))
                new_worker.start()
                merge_workers[idx] = new_worker
        if not reader.is_alive():
            print "respawn reader"
            reader.join()
            reader = multiprocessing.Process(target=start_reader)
            reader.start()
