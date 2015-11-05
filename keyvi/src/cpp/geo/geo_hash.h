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
 * geo_hash.h
 *
 * inspired by https://github.com/lyokato/libgeohash/blob/master/src/geohash.c
 * rewritten into header-only file
 *
 *  Created on: Nov 5, 2015
 *      Author: hendrik
 */

#ifndef GEO_HASH_H_
#define GEO_HASH_H_

namespace keyvi {
namespace geo {

static const char KEYVI_GEO_BASE32_ENCODE_TABLE[33] = "0123456789bcdefghjkmnpqrstuvwxyz";
static const char KEYVI_GEO_BASE32_DECODE_TABLE[44] = {
    /* 0 */   0, /* 1 */   1, /* 2 */   2, /* 3 */   3, /* 4 */   4,
    /* 5 */   5, /* 6 */   6, /* 7 */   7, /* 8 */   8, /* 9 */   9,
    /* : */  -1, /* ; */  -1, /* < */  -1, /* = */  -1, /* > */  -1,
    /* ? */  -1, /* @ */  -1, /* A */  -1, /* B */  10, /* C */  11,
    /* D */  12, /* E */  13, /* F */  14, /* G */  15, /* H */  16,
    /* I */  -1, /* J */  17, /* K */  18, /* L */  -1, /* M */  19,
    /* N */  20, /* O */  -1, /* P */  21, /* Q */  22, /* R */  23,
    /* S */  24, /* T */  25, /* U */  26, /* V */  27, /* W */  28,
    /* X */  29, /* Y */  30, /* Z */  31
};


#define KEYVI_GEO_REFINE_RANGE(range, bits, offset) \
    if (((bits) & (offset)) == (offset)) \
        (range)->min = ((range)->max + (range)->min) / 2.0; \
    else \
        (range)->max = ((range)->max + (range)->min) / 2.0;

#define KEYVI_GEO_SET_BIT(bits, mid, range, value, offset) \
    mid = ((range)->max + (range)->min) / 2.0; \
    if ((value) >= mid) { \
        (range)->min = mid; \
        (bits) |= (0x1 << (offset)); \
    } else { \
        (range)->max = mid; \
        (bits) |= (0x0 << (offset)); \
    }

class GeoHash final {
 public:

  typedef struct {
      double max;
      double min;
  } GEOHASH_range;

  typedef struct {
      GEOHASH_range latitude;
      GEOHASH_range longitude;
  } GEOHASH_area;

  static std::string Encode(double lat, double lon, size_t len){
    unsigned int i;
    std::string hash;
    unsigned char bits = 0;
    double mid;
    GEOHASH_range lat_range = {  90,  -90 };
    GEOHASH_range lon_range = { 180, -180 };

    double val1, val2, val_tmp;
    GEOHASH_range *range1, *range2, *range_tmp;

    if (lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0) {
      throw std::invalid_argument("Invalid geo coordinates");
    }

    // the max size is 22, taken from origin
    if (len > 22){
      throw std::invalid_argument("Unsupported geohash precission");
    }

    hash.resize(len);

    val1 = lon; range1 = &lon_range;
    val2 = lat; range2 = &lat_range;

    for (i=0; i < len; i++) {
      bits = 0;
      KEYVI_GEO_SET_BIT(bits, mid, range1, val1, 4);
      KEYVI_GEO_SET_BIT(bits, mid, range2, val2, 3);
      KEYVI_GEO_SET_BIT(bits, mid, range1, val1, 2);
      KEYVI_GEO_SET_BIT(bits, mid, range2, val2, 1);
      KEYVI_GEO_SET_BIT(bits, mid, range1, val1, 0);

      hash[i] = KEYVI_GEO_BASE32_ENCODE_TABLE[bits];

      val_tmp   = val1;
      val1      = val2;
      val2      = val_tmp;
      range_tmp = range1;
      range1    = range2;
      range2    = range_tmp;
    }

    return hash;
  }

  static GEOHASH_area Decode(const std::string& hash)
  {
    const char *p;
    unsigned char c;
    char bits;
    GEOHASH_area area;
    GEOHASH_range *range1, *range2, *range_tmp;

    area.latitude.max   =   90;
    area.latitude.min   =  -90;
    area.longitude.max =  180;
    area.longitude.min = -180;

    range1 = &area.longitude;
    range2 = &area.latitude;

    p = hash.c_str();

    while (*p != '\0') {

      c = toupper(*p++);
      if (c < 0x30) {
        throw std::invalid_argument("Invalid character in geohash.");
      }
      c -= 0x30;
      if (c > 43) {
       throw std::invalid_argument("Invalid character in geohash.");
      }
      bits = KEYVI_GEO_BASE32_DECODE_TABLE[c];
      if (bits == -1) {
        throw std::invalid_argument("Invalid character in geohash.");
      }

      KEYVI_GEO_REFINE_RANGE(range1, bits, 0x10);
      KEYVI_GEO_REFINE_RANGE(range2, bits, 0x08);
      KEYVI_GEO_REFINE_RANGE(range1, bits, 0x04);
      KEYVI_GEO_REFINE_RANGE(range2, bits, 0x02);
      KEYVI_GEO_REFINE_RANGE(range1, bits, 0x01);

      range_tmp = range1;
      range1    = range2;
      range2    = range_tmp;
    }
    return area;
  }
};


} /* namespace geo */
} /* namespace keyvi */

#endif /* GEO_HASH_H_ */
