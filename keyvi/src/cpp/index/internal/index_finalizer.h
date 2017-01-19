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
#include <ctime>
#include <thread>
#include <condition_variable>


#include "dictionary/dictionary_compiler.h"
#include "dictionary/dictionary_types.h"

#define ENABLE_TRACING
#include "dictionary/util/trace.h"


namespace keyvi {
namespace index {
namespace internal {

class IndexFinalizer final {
	typedef std::function<void (const std::string &)> finalizer_callback_t;

public:
	IndexFinalizer(/*const finalizer_callback_t finalizer_callback*/)
		:compiler_(), finalizer_callback_(/*finalizer_callback*/), do_flush_(false), flush_cond_(),
		 write_counter_(0)
	{
	}

	void StartFinalizerThread(){
		if (stop_finalizer_thread_ == false) {
			// already runs
			return;
		}

		stop_finalizer_thread_ = false;
		finalizer_thread_ = std::thread(&IndexFinalizer::Finalizer,this);
	}

	// todo: should not be public
	void StopFinalizerThread(){
		stop_finalizer_thread_ = true;
		if(finalizer_thread_.joinable()) {
			finalizer_thread_.join();
		}
	}

	dictionary::JsonDictionaryCompilerSmallData* GetCompiler(){
		if (compiler_.get() == nullptr) {
			compiler_.reset(new dictionary::JsonDictionaryCompilerSmallData());
		}

		return compiler_.get();
	}

	void Flush(bool async = true) {
		if (async) {
			do_flush_ = true;
			flush_cond_.notify_one();
			return;
		}//  else

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
	std::condition_variable flush_cond_;
	std::thread finalizer_thread_;
	std::atomic_bool stop_finalizer_thread_;
	size_t write_counter_;

	void Finalizer() {
		 while(!stop_finalizer_thread_){

			 TRACE("Finalizer, check for finalization.");
			 // reload
			 //ReloadIndex();

			 // sleep for some time
			 std::this_thread::sleep_for( std::chrono::seconds(1) );
		 }
	}

};

} /* namespace internal */
} /* namespace index */
} /* namespace keyvi */



#endif /* KEYVI_INDEX_INTERNAL_INDEX_FINALIZER_H_ */
