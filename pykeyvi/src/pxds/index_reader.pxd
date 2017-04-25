from libcpp.string  cimport string as libcpp_utf8_string
from libcpp cimport bool
        
cdef extern from "index/index_reader.h" namespace "keyvi::index":
    cdef cppclass IndexReader:
        IndexReader(libcpp_utf8_string) except+
        bool Contains(libcpp_utf8_string) except+
        