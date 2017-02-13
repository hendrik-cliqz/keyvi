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
 * readonly_segment.h
 *
 *  Created on: Jan 11, 2017
 *      Author: hendrik
 */

#ifndef KEYVI_INDEX_READONLY_SEGMENT_H_
#define KEYVI_INDEX_READONLY_SEGMENT_H_

#include <set>
#include <string>

#include "dictionary/dictionary.h"

namespace keyvi {
namespace index {

class ReadOnlySegment {
 public:
  explicit ReadOnlySegment(const std::string& filename, const bool load = true)
      : filename_(filename), deleted_keys_(), dictionary_() {
    if (load) {
      Load();
    }
  }

  dictionary::dictionary_t& operator*() { return dictionary_; }

  const std::string& GetFilename() const { return filename_; }

 private:
  std::string filename_;
  std::set<std::string> deleted_keys_;
  dictionary::dictionary_t dictionary_;

  void Load() { dictionary_.reset(new dictionary::Dictionary(filename_)); }
};

} /* namespace index */
} /* namespace keyvi */

/*
class ReadOnlySegment(object):
    def __init__(self, file_name, load=True):
        # todo: fix in pykeyvi
        if type(file_name) == unicode:
            file_name = file_name.encode("utf-8")
        self.file_name = file_name
        self.deleted_keys = []
        self.d = None
        if load:
            self.load()

    def load(self):
self.d = pykeyvi.Dictionary(self.file_name)
*/

#endif /* KEYVI_INDEX_READONLY_SEGMENT_H_ */
