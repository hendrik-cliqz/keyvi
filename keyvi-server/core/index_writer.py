# -*- coding: utf-8 -*-

import os
import threading
import multiprocessing
import logging
import pykeyvi
import time
import json
from shutil import move
import index_reader

def _get_segment_name(index_dir, prefix='master'):
    filename = os.path.join(index_dir, "{}-{}-{}.kv".format(prefix, int(time.time() * 1000000), os.getpid()))
    if type(filename) == unicode:
        filename = filename.encode("utf-8")
    return filename


def _merge(merge_job):
    merger = pykeyvi.JsonDictionaryMerger()
    for f in merge_job.merge_list:
        # todo: fix in pykeyvi
        if type(f) == unicode:
            f = f.encode("utf-8")
        merger.Add(f)

    merger.Merge(merge_job.merge_file)
    return

class MergeJob(object):
    def __init__(self, process=None, start_time=0, merge_list=[], merge_file=None, merge_completed=False):
        self.process = process
        self.start_time = start_time
        self.merge_list = merge_list
        self.merge_file = merge_file
        self.merge_completed = merge_completed

class IndexWriter(index_reader.IndexReader):
    class IndexerThread(threading.Thread):
        def __init__(self, writer, logger, commit_interval=1):
            self._writer = writer
            self.commit_interval = commit_interval
            self._delay = self.commit_interval
            self.last_commit = 0
            self.max_parallel_merges = 2
            self.log = logger
            self.merge_processes = []
            self.log.info("Index Thread started")
            super(IndexWriter.IndexerThread, self).__init__()

        def run(self):
            sleep_time = self._delay
            while True:
                time.sleep(sleep_time)
                self._finalize_merge()
                self._run_merge()
                sleep_time = self._commit_checked()

        def _commit_checked(self):
            now = time.time()
            if now - self.last_commit >= self._delay:
                self.commit()
                return self._delay
            else:
                return now - self.last_commit

        def _finalize_merge(self):
            any_merge_finalized = False

            for merge_job in self.merge_processes:
                if not merge_job.process.is_alive():
                    self._writer._finalize_merge(merge_job)
                    any_merge_finalized = True
                    merge_job.merge_completed = True
                    merge_job.process.join()

            if any_merge_finalized:
                self.merge_processes[:] = [m for m in self.merge_processes if not m.merge_completed]


        def _run_merge(self):
            if len(self.merge_processes) == self.max_parallel_merges:
                return

            to_merge = self._writer.find_merges()

            if len(to_merge) > 1:
                self.log.info("Start merge of {} segments".format(len(to_merge)))

                merge_job = MergeJob(start_time=time.time(), merge_list=to_merge,
                                     merge_file=_get_segment_name(self._writer.index_dir),
                                     merge_completed=False,
                                     process=None)

                p = multiprocessing.Process(target=_merge, args=(merge_job, ))
                merge_job.start_time = time.time()
                p.start()
                merge_job.process = p

                self.merge_processes.append(merge_job)

        def compile(self):
            if self._writer.compiler is None:
                return

            compiler = self._writer.compiler
            self._writer.compiler = None

            # check it again, to be sure
            if compiler is None:
                return
            self.log.info("creating segment")

            compiler.Compile()
            filename = _get_segment_name(self._writer.index_dir)
            compiler.WriteToFile(filename + ".part")
            os.rename(filename+".part", filename)

            self._writer.register_new_segment(filename)

            self.last_commit = time.time()

        def commit(self, async=True):
            if async:
                # todo: implement async
                self.compile()
            else:
                self.compile()

    def __init__(self, index_dir="kv-index", segment_write_interval=10, segment_write_trigger=10000):

        super(IndexWriter, self).__init__(index_dir, refresh_interval=0,
                                          logger=logging.getLogger("kv-writer"))
        self.log.info('Writer started')

        self.segments_in_merger = {}
        self.segments = []
        self.segments_marked_for_merge = []
        self.write_counter = 0
        self.merger_lock = threading.RLock()

        # todo: lock index (file lock)
        self.segment_write_trigger = segment_write_trigger
        self.compiler = None

        self.load_or_create_index()
        self._load_segments()

        self._finalizer = IndexWriter.IndexerThread(self, logger=self.log, commit_interval=segment_write_interval)
        self._finalizer.start()


    def load_or_create_index(self):
        if not self.load_index():
            self.log.info('No index found, creating it.')
            if not os.path.exists(self.index_dir):
                os.mkdir(self.index_dir)
            self._write_toc()

    def _init_lazy_compiler(self):
        if not self.compiler:
            self.compiler = pykeyvi.JsonDictionaryCompiler(1024*1024*10, {"stable_insert": "true"})
            self.write_counter = 0

    def find_merges(self):
        if (len(self.segments) - len(self.segments_marked_for_merge)) < 2:
            #LOG.info("skip merge, to many items in queue or to few segments")
            return []

        to_merge = []
        with self.merger_lock:

            for segment in list(self.segments):
                if segment not in self.segments_marked_for_merge:
                    self.log.info("add to merge list {}".format(segment))
                    to_merge.append(segment)

            if len(to_merge) > 1:
                self.segments_marked_for_merge.extend(to_merge)
                to_merge.reverse()

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

    def _load_segments(self):
        ld = []
        for f in self.segments:
            filename = f.encode("utf-8")
            self.log.info("Loading: @@@{}@@@".format(filename))
            ld.append(pykeyvi.Dictionary(filename))
            self.log.info("load dictionary {}".format(filename))
        self.loaded_dicts = ld

    def register_new_segment(self, new_segment):
        # add new segment
        with self.merger_lock:
            self.log.info("add {}".format(new_segment))

            self.segments.append(new_segment)
            self.loaded_dicts.append(pykeyvi.Dictionary(new_segment))
        # re-write toc to make new segment available
        self._write_toc()
        return

    def _finalize_merge(self, merge_job):
        self.log.info("finalize_merge called")
        if merge_job.process.exitcode != 0:
            self.log.warning("merge failed, recover: put merge list back")
            # remove from marker list
            with self.merger_lock:
                for item in merge_job.merge_list:
                    self.segments_marked_for_merge.remove(item)

            # todo: should merger run with lower load next time???

            return

        try:
            self.log.info("finalize merge, put it into the index")
            new_segments = []
            new_loaded_dicts = []
            merged = False
            with self.merger_lock:
                for s,d in zip(self.segments, self.loaded_dicts):
                    if s in merge_job.merge_list:
                        if not merged:
                            # found the place where merged segment should go in
                            new_segments.append(merge_job.merge_file)
                            new_loaded_dicts.append(pykeyvi.Dictionary(merge_job.merge_file))
                            merged = True
                    else:
                        new_segments.append(s)
                        new_loaded_dicts.append(d)


                self.log.info("Segments: {}".format(new_segments))
                self.segments = new_segments
                self.loaded_dicts = new_loaded_dicts

                # remove from marker list
                for item in merge_job.merge_list:
                    self.segments_marked_for_merge.remove(item)

            self._write_toc()

            # delete old files
            for f in merge_job.merge_list:
                os.remove(f)

        except:
            self.log.exception("Failed to finalize index")
            raise

    def set(self, key, value):
        if key is None:
            return

        self._init_lazy_compiler()
        self.compiler.Add(key, value)
        self.write_counter += 1

        if self.write_counter >= self.segment_write_trigger:
            self._finalizer.commit()

        return

    def setnx(self, key, value):
        """
        set key to value only if key does not exist

        :param key:
        :param value:
        :return:
        """
        for d in reversed(self.loaded_dicts):
            if key in d:
                return
        self.set(key, value)
        return

    def mset(self, key_value_pairs):
        for key, value in key_value_pairs:
            if key is None:
                continue
            self._init_lazy_compiler()
            self.compiler.Add(key, value)
            self.write_counter += 1
        if self.write_counter >= self.segment_write_trigger:
            self._finalizer.commit()

    def mset_bulk(self, client_token, key_value_pairs, optimistic=False):
        for key, value in key_value_pairs:
            if type(key) == unicode:
                key = key.encode("utf-8")
            if type(value) == unicode:
                value = value.encode("utf-8")

            # todo: use bulk compiler
            if self.compiler is None:
                self.compiler=self._init_lazy_compiler()
            self.compiler.Add(key, value)

    def delete(self, key):
        """
        delete key

        :param key:
        :return:
        """

        for d in reversed(self.loaded_dicts):
            if key in d:
                # mark key for delete

                return

        return

    def commit(self, async=True):
        self._finalizer.commit(async=async)
