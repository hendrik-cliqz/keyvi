# -*- coding: utf-8 -*-

import json
import logging
import os
import pykeyvi
import gevent


class ReadOnlySegment(object):
    def __init__(self, file_name, load=True):
        self.file_name = file_name
        self.d = pykeyvi.Dictionary(file_name)
        self.deleted_keys = []

    def load(self):
        self.d = pykeyvi.Dictionary(self.file_name)


class IndexReader(object):
    class IndexRefresh(gevent.Greenlet):
        def __init__(self, reader, interval=1):
            self._reader = reader
            self._delay = interval
            super(IndexReader.IndexRefresh, self).__init__()
        def _run(self):
            sleep_time = self._delay
            while True:
                gevent.sleep(sleep_time)
                try:
                    self._reader.check_toc()
                except:
                    self._reader.log.exception('Failed to refresh index')

    def __init__(self, index_dir="kv-index", refresh_interval=1, logger=None):
        self.index_dir = index_dir
        self.index_file = os.path.join(index_dir, "index.toc")
        if logger:
            self.log = logger
        else:
            self.log = logging.getLogger("kv-reader")
        self.log.info('Reader started, Index: {}, Refresh: {}'.format(self.index_dir, refresh_interval))
        self.toc = {}
        self.loaded_dicts = []
        self.last_stat_rs_mtime = 0

        if refresh_interval > 0:
            self._refresh = IndexReader.IndexRefresh(self, refresh_interval)
            self._refresh.start()

    def load_index(self):
        if not os.path.exists(self.index_file):
            self.log.warning("No Index found")
            return False

        try:
            toc = '\n'.join(open(self.index_file).readlines())
            toc = json.loads(toc)
            self.segments = toc.get('files', [])
            self.log.info("loaded index")

        except Exception, e:
            self.log.exception("failed to load index")
            raise

        return True


    def check_toc(self):
        try:
            stat_rs = os.stat(self.index_file)
        except:
            self.log.exception("could not load toc")
            return

        if stat_rs.st_mtime != self.last_stat_rs_mtime:
            self.log.info("reload toc")
            self.last_stat_rs_mtime = stat_rs.st_mtime
            self.load_index()
            new_list_of_dicts = []
            self.log.info("loading files")
            files = self.toc.get('files', [])
            files.reverse()
            for f in files:
                filename = f.encode("utf-8")
                self.log.info("Loading: @@@{}@@@".format(filename))
                new_list_of_dicts.append(pykeyvi.Dictionary(filename))
                self.log.info("load dictionary {}".format(filename))
            self.loaded_dicts = new_list_of_dicts

    def get(self, key):
        if key is None:
            return None

        for d in self.loaded_dicts:
            m = d.get(key)
            if m is not None:
                return m.dumps()
        return None

    def exists(self, key):
        if key is None:
            return False

        for d in self.loaded_dicts:
            if key in d:
                return True
        return False

    def reload(self):
        self.check_toc()
