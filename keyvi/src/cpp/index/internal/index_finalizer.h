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
#include <algorithm>
#include <condition_variable>
#include <ctime>
#include <list>
#include <string>
#include <thread>
#include <vector>

#include "dictionary/dictionary_compiler.h"
#include "dictionary/dictionary_types.h"
#include "index/internal/segment.h"
#include "index/internal/merge_job.h"

#define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace index {
namespace internal {

class IndexFinalizer final {
  typedef std::function<void(const std::string&)> finalizer_callback_t;

 public:
  explicit IndexFinalizer(const std::string& index_directory,
                          const std::chrono::duration<double>& flush_interval
                          = std::chrono::milliseconds(1000))
      : compiler_(),
        do_flush_(false),
        flush_cond_(),
        write_counter_(0),
        stop_finalizer_thread_(true),
        flush_interval_(flush_interval) {
    index_directory_ = index_directory;
  }

  void StartFinalizerThread() {
    if (stop_finalizer_thread_ == false) {
      // already runs
      return;
    }

    stop_finalizer_thread_ = false;
    TRACE("Start Finalizer thread");
    finalizer_thread_ = std::thread(&IndexFinalizer::Finalizer, this);
  }

  // todo: should not be public
  void StopFinalizerThread() {
    stop_finalizer_thread_ = true;
    if (finalizer_thread_.joinable()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      finalizer_thread_.join();
      TRACE("finalizer thread joined");
      TRACE("Open merges: %ld", merge_jobs_.size());
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
      auto tp = std::chrono::system_clock::now();
      TRACE("notify %ld", tp.time_since_epoch());
      flush_cond_.notify_one();
      return;
    }  //  else

    // TODO: blocking implementation
  }

  void CheckForCommit() {
    if (++write_counter_ > 1000) {
      // todo: make non-blocking
      Flush();
      write_counter_ = 0;
    }
  }

 private:
  std::shared_ptr<dictionary::JsonDictionaryCompilerSmallData> compiler_;
  std::atomic_bool do_flush_;
  std::recursive_mutex index_mutex_;
  std::mutex flush_cond_mutex_;
  std::condition_variable flush_cond_;
  std::thread finalizer_thread_;
  std::atomic_bool stop_finalizer_thread_;
  size_t write_counter_;
  std::vector<Segment> segments_;
  boost::filesystem::path index_directory_;
  std::list<MergeJob> merge_jobs_;
  std::chrono::system_clock::time_point last_flush_;
  std::chrono::duration<double> flush_interval_;
  std::chrono::duration<double> finalizer_poll_interval_ =
      std::chrono::milliseconds(50);
  size_t max_concurrent_merges = 2;


  void Finalizer() {
    std::unique_lock<std::mutex> l(flush_cond_mutex_);
    TRACE("Finalizer loop");
    while (!stop_finalizer_thread_) {
      TRACE("Finalizer, check for finalization.");
      FinalizeMerge();
      RunMerge();

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
      flush_cond_.wait_for(l, finalizer_poll_interval_);
      auto tp = std::chrono::system_clock::now();
      TRACE("wakeup finalizer %s %ld ***", l.owns_lock() ? "true":"false",
          tp.time_since_epoch());

      if (do_flush_ == true || tp - last_flush_ > flush_interval_) {
        do_flush_ = false;
        Compile();
        last_flush_ = tp;
      }
    }

    TRACE("Finalizer loop stop");
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

    if (compiler_.get() == nullptr) {
      return;
    }

    std::shared_ptr<dictionary::JsonDictionaryCompilerSmallData> compiler;
    TRACE("compile");

    // TODO(hendrik) this is not safe, no guarantee that compiler
    // is not in use in the writer thread
    // swap already in check for commit into another member, set an atomic bool
    // if compile is already going on think about throttling
    compiler_.swap(compiler);

    // additional check
    if (compiler.get() == nullptr) {
      return;
    }

    // we need to somehow ensure that nobody writes to it
    compiler->Compile();

    boost::filesystem::path p(index_directory_);
    p /= boost::filesystem::unique_path("%%%%-%%%%-%%%%-%%%%.kv");

    TRACE("write to file %s %s", p.string().c_str(),
          p.filename().string().c_str());

    compiler->WriteToFile(p.string());

    // free up resources
    compiler.reset();
    Segment w(p);
    // register segment
    RegisterSegment(w);

    TRACE("Segment compiled and registered");
  }

  /**
   * Check if any merge process is done and finalize if necessary
   */
  void FinalizeMerge() {
    bool any_merge_finalized = false;
    TRACE("Finalize Merge");
    for (MergeJob& p : merge_jobs_) {
      if (p.TryFinalize()) {
        if (p.Successful()) {
          TRACE("rewriting segment list");
          any_merge_finalized = true;

          std::lock_guard<std::recursive_mutex> lock(index_mutex_);

          // remove old segments and replace it with new one
          std::vector<Segment> new_segments;
          bool merged_new_segment = false;
          std::copy_if(segments_.begin(),
                       segments_.end(),
                       std::back_inserter(new_segments),
                       [&new_segments, &merged_new_segment, &p]
                        (const Segment& s) {

                          TRACE("checking %s", s.GetFilename().c_str());
                          if (std::count_if(p.Segments().begin(),
                                p.Segments().end(),
                                [s](const Segment& s2) {
                                  return s2.GetFilename() == s.GetFilename();
                                })) {

                            if (!merged_new_segment) {
                              new_segments.push_back(p.MergedSegment());
                              merged_new_segment = true;
                            }
                            return false;
                          }
                          return true;
          });
          TRACE("merged segment %s", p.MergedSegment().GetFilename().c_str());
          TRACE("1st segment after merge: %s", new_segments[0].GetFilename().c_str());


          segments_.swap(new_segments);
          WriteToc();

          // delete old segment files
          for (const Segment s : p.Segments()) {
            TRACE("delete old file: %s", s.GetFilename().c_str());
            std::remove(s.GetFilename().c_str());
          }

          p.SetMerged();

        } else {
          // the merge process failed
          TRACE("merge failed, reset markers");
          // mark all segments as mergable again
          for (auto s : p.Segments()) {
            s.UnMarkMerge();
          }

          // todo throttle strategy?
        }
      }
      // else the merge is still running, maybe check how long it already runs
    }


    if (any_merge_finalized) {
      TRACE("delete merge job");
      std::remove_if(merge_jobs_.begin(),
                     merge_jobs_.end(),
                     [](const MergeJob& j) {return j.Merged();});
    }
    // merge_processes_.erase()

    /*any_merge_finalized = False

                for merge_job in self.merge_processes:
                    if not merge_job.process.is_alive():
                        self._writer._finalize_merge(merge_job)
                        any_merge_finalized = True
                        merge_job.merge_completed = True
                        merge_job.process.join()

                if any_merge_finalized:
                    self.merge_processes[:] = [m for m in self.merge_processes if
    not m.merge_completed]*/
  }

  /**
   * Run a merge if mergers are available and segments require merge
   */
  void RunMerge() {
    // to few segments, return
    if (segments_.size() <=1) {
      return;
    }

    if (merge_jobs_.size() == max_concurrent_merges) {
      // to many merges already running, so throttle
      return;
    }

    // todo: merger semaphore check

    std::vector<Segment> to_merge;

    for (auto s : segments_) {
      if (!s.MarkedForMerge()) {
        TRACE("Add to merge list %s", s.GetFilename().c_str());
        to_merge.push_back(s);
      }

      /*
      if not segment.marked_for_merge():
                          self.log.info("add to merge list {}".format(segment))
                          to_merge.append(segment)

                  if len(to_merge) > 1:
                      new_segment = Segment(_get_segment_name(self.index_dir))

                      for segment in to_merge:
                          segment.mark_for_merge(new_segment)

                      to_merge.reverse()

                      merge_job = MergeJob(start_time=time.time(),
      merge_list=to_merge,
      }
    }*/
    }

    if (to_merge.size() < 1) {
     return;
    }

    TRACE("enough segments found for merging");
    boost::filesystem::path p(index_directory_);
    p /= boost::filesystem::unique_path("%%%%-%%%%-%%%%-%%%%.kv");
    Segment parent_segment(p, false);


    for (auto s : to_merge) {
      s.MarkMerge(&parent_segment);
    }

    // reverse the list
    std::reverse(to_merge.begin(), to_merge.end());

    merge_jobs_.emplace_back(to_merge, parent_segment);
    merge_jobs_.back().Run();

    // actually run the merge


    /*
    if len(self.merge_processes) == self.max_parallel_merges:
                    return

                merge_job = self._writer.find_merges()

                if merge_job is not None:
                    self.log.info("Start merge of {}
    segments".format(len(merge_job.merge_list)))

                    p = multiprocessing.Process(target=_merge, args=(merge_job, ))
                    merge_job.start_time = time.time()
                    p.start()
                    merge_job.process = p

                    self.merge_processes.append(merge_job)
                    */

  }

  void RegisterSegment(Segment segment) {
    std::lock_guard<std::recursive_mutex> lock(index_mutex_);
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
    std::lock_guard<std::recursive_mutex> lock(index_mutex_);
    TRACE("write new TOC");

    boost::property_tree::ptree ptree;
    boost::property_tree::ptree files;

    TRACE("Number of segments: %ld", segments_.size());

    for (auto s : segments_) {
      TRACE("put %s", s.GetFilename().c_str());
      // files.put_value(s.GetFilename());
      boost::property_tree::ptree sp;
      sp.put("", s.GetFilename());
      files.push_back(std::make_pair("", sp));
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
