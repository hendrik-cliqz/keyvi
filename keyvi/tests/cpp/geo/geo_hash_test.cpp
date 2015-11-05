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
 * geo_hash_test.cpp
 *
 *  Created on: Nov 5, 2015
 *      Author: hendrik
 */

#include <boost/test/unit_test.hpp>
#include "geo/geo_hash.h"

namespace keyvi {
namespace geo {

BOOST_AUTO_TEST_SUITE( GeoTests )

BOOST_AUTO_TEST_CASE( encode ) {
  BOOST_CHECK_EQUAL("u283bmvkvwg7", GeoHash::Encode(48.152555, 11.619999, 12));
  BOOST_CHECK_EQUAL("c216ne", GeoHash::Encode(45.37, -121.7, 6));
  BOOST_CHECK_EQUAL("c23nb62w20sth", GeoHash::Encode(47.6062095, -122.3320708, 13));
  BOOST_CHECK_EQUAL("xn774c06kdtve", GeoHash::Encode(35.6894875, 139.6917064, 13));
  BOOST_CHECK_EQUAL("r3gx2f9tt5sne", GeoHash::Encode(-33.8671390, 151.2071140, 13));
  BOOST_CHECK_EQUAL("gcpuvpk44kprq", GeoHash::Encode(51.5001524, -0.1262362, 13));
}

BOOST_AUTO_TEST_CASE( decode ) {
  GeoHash::GEOHASH_area area;
  area = GeoHash::Decode("u283bmvkvwg7");

  BOOST_CHECK(area.latitude.min < 48.152555);
  BOOST_CHECK(area.latitude.max > 48.152555);
  BOOST_CHECK(area.longitude.min < 11.619999);
  BOOST_CHECK(area.longitude.max > 11.619999);
}



BOOST_AUTO_TEST_SUITE_END()

} /* namespace geo */
} /* namespace keyvi */

