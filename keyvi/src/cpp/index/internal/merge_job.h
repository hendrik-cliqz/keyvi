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
 * merge_process.h
 *
 *  Created on: Feb 17, 2017
 *      Author: hendrik
 */

#ifndef KEYVI_INDEX_INTERNAL_MERGE_PROCESS_H_
#define KEYVI_INDEX_INTERNAL_MERGE_PROCESS_H_

#include <string>
#include <thread>
#include <vector>

#include "process.hpp"
#include "dictionary/dictionary_merger.h"
#include "dictionary/dictionary_types.h"
#include "dictionary/fsa/internal/sparse_array_persistence.h"
#include "dictionary/fsa/internal/json_value_store.h"

#include "index/internal/segment.h"

#define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace index {
namespace internal {

class MergeJob final {
  struct MergeJobPayload {
    MergeJobPayload(const std::vector<Segment>& segments,
                    const Segment& new_segment)
    : segments_(segments), new_segment_(new_segment) {}

    std::vector<Segment> segments_;
    Segment new_segment_;
    std::chrono::time_point<std::chrono::system_clock> start_time_;
    std::chrono::time_point<std::chrono::system_clock> end_time_;
    int exit_code_ = -1;
    bool merge_done = false;
  };

 public:
  MergeJob(const std::vector<Segment>& segments, const Segment& new_segment)
  : payload_(segments, new_segment) {}

  void Run() {
    MergeJobPayload* job = &payload_;

    job_thread_ = std::thread([job]() {
      job->start_time_ = std::chrono::system_clock::now();

      misc::Process merge_process([job] {
          // dictionary::JsonDictionaryMerger merger();
          dictionary::DictionaryMerger<
          dictionary::fsa::internal::SparseArrayPersistence<>,
          dictionary::fsa::internal::JsonValueStore> m(
              dictionary::merger_param_t({{"memory_limit_mb", "10"}}));

          for (auto s : job->segments_) {
            m.Add(s.GetPath().string());
          }

          TRACE("merge done");
          m.Merge(job->new_segment_.GetPath().string());
          exit(0);
          });

      job->exit_code_ = merge_process.get_exit_status();
      job->end_time_ = std::chrono::system_clock::now();

      TRACE("Merge finished with %ld", job->exit_code_);
      });
  }

  bool isRunning() const {
    return !job_thread_.joinable();
  }

  bool TryFinalize() {
    if (job_thread_.joinable()) {
      job_thread_.join();

      return true;
    }

    return false;
  }

  bool Successful() {
    // todo: handle case when process crashed
    return payload_.exit_code_ == 0;
  }

  const std::vector<Segment>& Segments() const {
    return payload_.segments_;
  }

  const Segment& MergedSegment() const {
    return payload_.new_segment_;
  }

  void SetMerged() {
    payload_.merge_done = true;
  }

  const bool Merged() const {
    return payload_.merge_done;
  }

  // todo: ability to kill job/process

 private:
  MergeJobPayload payload_;
  std::thread job_thread_;
};

} /* namespace internal */
} /* namespace index */
} /* namespace keyvi */


#endif /* KEYVI_INDEX_INTERNAL_MERGE_PROCESS_H_ */
