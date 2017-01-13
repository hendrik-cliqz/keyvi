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
 * writable_segment.h
 *
 *  Created on: Jan 11, 2017
 *      Author: hendrik
 */



#ifndef SRC_CPP_INDEX_WRITABLE_SEGMENT_H_
#define SRC_CPP_INDEX_WRITABLE_SEGMENT_H_

#include "index/readonly_segment.h"


namespace keyvi {
namespace index {

class WritableSegment final: ReadOnlySegment {
public:
	WritableSegment(const std::string& filename, bool load = false)
    : ReadOnlySegment(filename, load), parent_segment_(nullptr) {

	}

	void MarkMerge(const WritableSegment* parent_segment) {
		parent_segment_ = parent_segment;
	}

	void UnMarkMerge() {
		parent_segment_ = nullptr;
	}

	bool MarkedForMerge() const {
		return parent_segment_ != nullptr;
	}



private:
  WritableSegment* parent_segment_;
};


} /* namespace index */
} /* namespace keyvi */
/*

 class Segment(index_reader.ReadOnlySegment):
    def __init__(self, file_name, load=False):
        super(Segment, self).__init__(file_name, load)
        self.parent_segment = None

    def marked_for_merge(self):
        return self.parent_segment is not None

    def mark_for_merge(self, parent_segment):
        self.parent_segment = parent_segment

    def unmark_for_merge(self):
        """
        Delete the merge marker, called in case of disaster (merge failure)
        :return:
        """
        self.parent_segment = None

    def delete_key(self, key):
        if self.parent_segment:
            self.parent_segment.delete_key(key)
        else:
self.deleted_keys.append(key)


 */


#endif /* SRC_CPP_INDEX_WRITABLE_SEGMENT_H_ */
