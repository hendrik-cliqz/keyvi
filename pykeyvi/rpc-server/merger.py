# -*- coding: utf-8 -*-

import json
import logging
import glob
from logging.handlers import RotatingFileHandler
from datetime import datetime
import operator
import os
from shutil import move
import time
import pykeyvi
from multiprocessing import Process


class Merger(object):
    def __init__(self, index_dir="kv-index"):
        self.index_dir = index_dir
        self.index_file = os.path.join(index_dir, "index.toc")
        self.log = self._create_logger()
        self.log.info('Merger started')

    def _create_logger(self):
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

    def _write_toc(self, filenames):
        toc = json.dumps({"files": filenames})
        fd = open("index.toc.new", "w")
        fd.write(toc)
        fd.close()
        move("index.toc.new", self.index_file)

    def run(self):
        while (True):
            files = glob.glob(self.index_dir+"/*.kv")
            if len(files) < 2:
                time.sleep(2)
                continue
            else:
                self._write_toc(files)

                merger = pykeyvi.JsonDictionaryMerger()
                for f in sorted(files):
                    self.log.info('add to merger: {}'.format(f))
                    merger.Add(f)

                filename=os.path.join(self.index_dir, "{}-{}.kv".format(datetime.now(), os.getpid()))
                merger.Merge(filename)
                self._write_toc([filename])

                # delete old files
                for f in files:
                    os.remove(f)

if __name__ == '__main__':
    m = Merger()
    m.run()