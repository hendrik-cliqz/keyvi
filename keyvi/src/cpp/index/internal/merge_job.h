/* * keyvi - A key value store.
 *
 * Copyright 2015 Hendrik Muhs<hendrik.muhs@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



/*
 * index_finalizer.h
 *
 *  Created on: Jan 18, 2017
 *      Author: hendrik
 */

#ifndef KEYVI_INDEX_INTERNAL_MERGE_JOB_H_
#define KEYVI_INDEX_INTERNAL_MERGE_JOB_H_

#include <chrono>
#include <vector>

#include "index/internal/writable_segment.h"

#define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace index {
namespace internal {

class MergeJob final {
 public:
  MergeJob(const std::chrono::time_point<std::chrono::system_clock> start_time,
           const std::vector<WritableSegment>& segments,
           const WritableSegment& new_segment)
  : start_time_(start_time),
   segments_(segments),
   new_segment_(new_segment),
   merge_completed_(false) {


  }

 private:
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::vector<WritableSegment> segments_;
  WritableSegment new_segment_;
  bool merge_completed_;

  /*
  self.process = process
          self.start_time = start_time
          self.merge_list = merge_list
          self.new_segment = new_segment
          self.merge_completed = merge_completed
          */

};

} /* namespace internal */
} /* namespace index */
} /* namespace keyvi */

#endif /* KEYVI_INDEX_INTERNAL_MERGE_JOB_H_ */
