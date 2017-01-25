//
// keyvi - A key value store.
//
// Copyright 2015 Hendrik Muhs<hendrik.muhs@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

/*
 * index_reader.h
 *
 *  Created on: Jan 11, 2017
 *      Author: hendrik
 */

#ifndef KEYVI_INDEX_INDEX_READER_H_
#define KEYVI_INDEX_INDEX_READER_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <string>
#include <thread>
#include <vector>

#include <boost/filesystem/operations.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "dictionary/dictionary.h"
#include "dictionary/fsa/internal/serialization_utils.h"
#include "dictionary/match.h"
#include "index/readonly_segment.h"

#define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace index {

class IndexReader final {
 public:
  IndexReader(const std::string index_directory,
              size_t refresh_interval = 1 /*, optional external logger*/)
      : index_toc_(), segments_(), stop_update_thread_(true) {
    index_directory_ = index_directory;

    index_toc_file_ = index_directory_;
    index_toc_file_ /= "index.toc";

    TRACE("Reader started, index TOC: %s", index_toc_file_.string().c_str());

    last_modification_time_ = 0;

    // TODO: start refresh thread
    /*		        if refresh_interval > 0:
                                self._refresh = IndexReader.IndexRefresh(self,
       refresh_interval)
                                self._refresh.start()
    */
    ReloadIndex();
  }

  ~IndexReader() {
    stop_update_thread_ = true;
    if (update_thread_.joinable()) {
      update_thread_.join();
    }
  }

  void StartUpdateWatcher() {
    if (stop_update_thread_ == false) {
      // already runs
      return;
    }

    stop_update_thread_ = false;
    update_thread_ = std::thread(&IndexReader::UpdateWatcher, this);
  }

  void StopUpdateWatcher() {
    stop_update_thread_ = true;
    if (update_thread_.joinable()) {
      update_thread_.join();
    }
  }

  dictionary::Match operator[](const std::string& key) const {
    dictionary::Match m;

    for (auto s : segments_) {
      m = (*s)->operator[](key);
      if (!m.IsEmpty()) {
        return m;
      }
    }

    return m;
  }

  bool Contains(const std::string& key) const {
    for (auto s : segments_) {
      if ((*s)->Contains(key)) {
        return true;
      }
    }

    return false;
  }

  void Reload() { ReloadIndex(); }

 private:
  boost::filesystem::path index_directory_;
  boost::filesystem::path index_toc_file_;
  std::time_t last_modification_time_;
  boost::property_tree::ptree index_toc_;
  std::vector<ReadOnlySegment> segments_;
  std::thread update_thread_;
  std::atomic_bool stop_update_thread_;

  void LoadIndex() {
    if (!boost::filesystem::exists(index_directory_)) {
      TRACE("No index found.");
      return;
    }
    TRACE("read toc");

    std::ifstream toc_fstream(index_toc_file_.string());

    TRACE("rereading %s", index_toc_file_.string().c_str());

    if (!toc_fstream.good()) {
      throw std::invalid_argument("file not found");
    }

    TRACE("read toc 2");

    boost::property_tree::read_json(toc_fstream, index_toc_);
    TRACE("index_toc loaded");
  }

  void ReloadIndex() {
    std::time_t t = boost::filesystem::last_write_time(index_toc_file_);

    if (t <= last_modification_time_) {
      TRACE("no modifications found");
      return;
    }

    TRACE("reload toc");
    last_modification_time_ = t;
    LoadIndex();

    TRACE("reading segments");

    std::vector<ReadOnlySegment> new_segments;

    for (boost::property_tree::ptree::value_type& f :
         index_toc_.get_child("files")) {
      boost::filesystem::path p(index_directory_);
      p /= f.second.data();
      new_segments.push_back(ReadOnlySegment(p.string()));
    }

    // reverse the list
    std::reverse(new_segments.begin(), new_segments.end());

    segments_.swap(new_segments);
    TRACE("Loaded new segments");
  }

  void UpdateWatcher() {
    while (!stop_update_thread_) {
      TRACE("UpdateWatcher: Check for new segments");
      // reload
      ReloadIndex();

      // sleep for some time
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
};

} /* namespace index */
} /* namespace keyvi */

/*
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
        self.log.info('Reader started, Index: {}, Refresh:
 {}'.format(self.index_dir, refresh_interval))
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

 */

#endif /* KEYVI_INDEX_INDEX_READER_H_ */
