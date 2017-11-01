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
#include <thread>
#include <boost/filesystem.hpp>

#include "index/index_writer.h"

int main(int argc, char** argv) {
  using boost::filesystem::temp_directory_path;
  using boost::filesystem::unique_path;

  auto tmp_path = temp_directory_path();
  tmp_path /= unique_path();
  keyvi::index::IndexWriter writer(tmp_path.string());

  for (int i = 0; i < 100000; ++i) {
    writer.Set("a", "{\"id\":" + std::to_string(i) + "}");
  }

  //std::this_thread::sleep_for(std::chrono::seconds(10));
}



