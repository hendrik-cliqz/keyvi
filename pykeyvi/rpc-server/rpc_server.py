# -*- coding: utf-8 -*-

import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import operator
import os
import pykeyvi
from mprpc import RPCClient

from mprpc import RPCServer

class KeyviServer(RPCServer):
    def __init__(self, index_dir="kv-index"):
        self.index_dir = index_dir
        self.index_file = os.path.join(index_dir, "index.toc")
        self.log = self._create_logger()
        self.log.info('Server started')
        self.toc = {}
        self.loaded_dicts = []
        self.compiler = None
        self.last_stat_rs_mtime = 0
        super(KeyviServer, self).__init__(pack_params={'use_bin_type': True}, tcp_no_delay=True)

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

    def _load_index_file(self):
        toc = '\n'.join(open(self.index_file).readlines())
        try:
            new_toc = json.loads(toc)
            self.toc = new_toc
            self.log.info("loaded toc")
        except Exception, e:
            self.log.exception("failed to load toc")

    def _check_toc(self):
        try:
            stat_rs = os.stat(self.index_file)
        except:
            self.log.exception("could not load toc")
            return


        if stat_rs.st_mtime != self.last_stat_rs_mtime:
            self.log.info("reload toc")
            self.last_stat_rs_mtime = stat_rs.st_mtime
            self._load_index_file()
            new_list_of_dicts = []
            self.log.info("loading files")
            for f in self.toc.get('files', []):
                filename = f.encode("utf-8")
                self.log.info("Loading: @@@{}@@@".format(filename))
                new_list_of_dicts.append(pykeyvi.Dictionary(filename))
                self.log.info("load dictionary {}".format(filename))
            self.loaded_dicts = new_list_of_dicts

    def get(self, key):
        if key is None:
            return None
        if type(key) == unicode:
            key = key.encode("utf-8")

        self._check_toc()

        for d in self.loaded_dicts:
            m = d.get(key)
            if m is not None:
                return m.dumps()
        return None

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

    def commit(self):
        if self.compiler is None:
            return
        self.compiler.Compile()
        filename=os.path.join(self.index_dir, "{}-{}.kv".format(datetime.now(), os.getpid()))
        self.compiler.WriteToFile(filename + ".part")
        os.rename(filename+".part", filename)
        self.compiler = None
        c = RPCClient('localhost', 6100)
        c.call('register_new_segment', filename)