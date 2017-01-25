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
 * index_finalizer.h
 *
 *  Created on: Jan 18, 2017
 *      Author: hendrik
 */

#ifndef KEYVI_INDEX_INTERNAL_INDEX_FINALIZER_H_
#define KEYVI_INDEX_INTERNAL_INDEX_FINALIZER_H_

#include <atomic>
#include <condition_variable>
#include <ctime>
#include <string>
#include <thread>
#include <vector>

#include "dictionary/dictionary_compiler.h"
#include "dictionary/dictionary_types.h"
#include "index/internal/writable_segment.h"

#define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace index {
namespace internal {

class IndexFinalizer final {
  typedef std::function<void(const std::string&)> finalizer_callback_t;

 public:
  IndexFinalizer(const std::string& index_directory

                 /*const finalizer_callback_t finalizer_callback*/)
      : compiler_(),
        finalizer_callback_(/*finalizer_callback*/),
        do_flush_(false),
        flush_cond_(),
        write_counter_(0) {
    index_directory_ = index_directory;
  }

  void StartFinalizerThread() {
    if (stop_finalizer_thread_ == false) {
      // already runs
      return;
    }

    stop_finalizer_thread_ = false;
    finalizer_thread_ = std::thread(&IndexFinalizer::Finalizer, this);
  }

  // todo: should not be public
  void StopFinalizerThread() {
    stop_finalizer_thread_ = true;
    if (finalizer_thread_.joinable()) {
      finalizer_thread_.join();
    }
  }

  dictionary::JsonDictionaryCompilerSmallData* GetCompiler() {
    if (compiler_.get() == nullptr) {
      // todo

      compiler_.reset(new dictionary::JsonDictionaryCompilerSmallData());
    }

    return compiler_.get();
  }

  void Flush(bool async = true) {
    if (async) {
      do_flush_ = true;
      flush_cond_.notify_one();
      return;
    }  //  else

    // TODO: blocking implementation
  }

  void CheckForCommit() {
    if (write_counter_ > 1000) {
      // todo: make non-blocking
      Flush();
    }
  }

 private:
  std::shared_ptr<dictionary::JsonDictionaryCompilerSmallData> compiler_;
  finalizer_callback_t finalizer_callback_;
  std::atomic_bool do_flush_;
  std::mutex index_mutex_;
  std::unique_lock<std::mutex> index_writer_lock_;
  std::condition_variable flush_cond_;
  std::thread finalizer_thread_;
  std::atomic_bool stop_finalizer_thread_;
  size_t write_counter_;
  std::vector<WritableSegment> segments_;
  boost::filesystem::path index_directory_;

  void Finalizer() {
    std::unique_lock<std::mutex> l(index_mutex_);

    while (!stop_finalizer_thread_) {
      TRACE("Finalizer, check for finalization.");
      // reload
      // ReloadIndex();
      /*
            time.sleep(self._delay)
                            self._finalize_merge()
                            self._run_merge()
                            now = time.time()

                            if self.run_commit or (int (now - self.last_commit)
         >=
            self.commit_interval):
                                self.log.info("Last commit: {} {} {}
            {}".format(self.last_commit, now - self.last_commit,
         self.commit_interval,
            self.run_commit))
                                self.run_commit = False
                                self.last_commit = time.time()
                                self.compile()
        */

      // sleep for some time or until woken up
      flush_cond_.wait_for(l, std::chrono::seconds(1));

      if (do_flush_ == true) {
        Compile();
      }
    }
  }

  void Compile() {
    /*
    if self._writer.compiler is None:
        return

    # limit number of compilers
    with self.compile_semaphore:

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

        # free up resource
        del compiler

    self._writer.register_new_segment(filename)
    */

    // TODO(hendrik): figure out how to make this thread-safe
    std::shared_ptr<dictionary::JsonDictionaryCompilerSmallData> compiler;
    compiler_.swap(compiler);

    compiler->Compile();

    boost::filesystem::path p(index_directory_);
    p /= boost::filesystem::unique_path("%%%%-%%%%-%%%%-%%%%.kv");

    compiler->WriteToFile(p.string());

    // free up resources
    compiler.reset();
    WritableSegment w(p.filename().string());
    // register segment
    RegisterSegment(w);
  }

  void RegisterSegment(WritableSegment segment) {
    // std::lock_guard<std::mutex> lock(index_mutex_);
    TRACE("add segment %s", segment.GetFilename().c_str());
    segments_.push_back(segment);
    WriteToc();
    /*
            # add new segment
            with self.merger_lock:
                self.log.info("add {}".format(new_segment))

                self.segments.append(Segment(new_segment, True))
            # re-write toc to make new segment available
            self._write_toc()
            return*/
  }

  void WriteToc() {
    /*
     * try:
            with self.merger_lock:
                self.log.info("write new TOC")
                files = [s.file_name for s in self.segments]
                toc = json.dumps({"files": files})
                fd = open("index.toc.new", "w")
                fd.write(toc)
                fd.close()
                move("index.toc.new", self.index_file)
        except:
            self.log.exception("failed to write toc")
            raise
     */
    // std::lock_guard<std::mutex> lock(index_mutex_);
    TRACE("write new TOC");

    boost::property_tree::ptree ptree;
    boost::property_tree::ptree files;

    for (auto s : segments_) {
      files.put_value(s.GetFilename());
    }

    ptree.add_child("files", files);
    boost::filesystem::path p(index_directory_);
    p /= "index.toc.part";

    boost::filesystem::path p2(index_directory_);
    p2 /= "index.toc";

    boost::property_tree::write_json(p.string(), ptree);
    boost::filesystem::rename(p, p2);
  }
};

} /* namespace internal */
} /* namespace index */
} /* namespace keyvi */

#endif /* KEYVI_INDEX_INTERNAL_INDEX_FINALIZER_H_ */
