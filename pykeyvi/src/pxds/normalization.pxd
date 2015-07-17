from libcpp.string cimport string as libcpp_string
from dictionary cimport Dictionary
from  smart_ptr cimport shared_ptr

cdef extern from "transform/fsa_transform.h" namespace "keyvi::transform":
    cdef cppclass FsaTransform:
        FsaTransform(shared_ptr[Dictionary]) except +
        libcpp_string Normalize(libcpp_string) nogil
