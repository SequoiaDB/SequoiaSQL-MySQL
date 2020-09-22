cmake_minimum_required(VERSION 2.8.7)

project(sdbvertool)

if(NOT SDB_DRIVER_DIR)
   message(FATAL_ERROR "SequoiaDB driver path is not set")
endif()

set(SDB_DRIVER_LIB_NAME libsdbcpp.so)

set(SDB_DRIVER_INCLUDE_DIR "${SDB_DRIVER_DIR}/include")
set(SDB_DRIVER_LIB_DIR "${SDB_DRIVER_DIR}/lib")

if(OPENSSL_LIB_DIR)
   set(OPENSSL_SSL_LIB_NAME libssl.a)
   set(OPENSSL_CRYPTO_LIB_NAME libcrypto.a)

   find_library(OPENSSL_SSL_LIB
               names ${OPENSSL_SSL_LIB_NAME}
               HINTS ${OPENSSL_LIB_DIR})

   if(${OPENSSL_SSL_LIB} STREQUAL "OPENSSL_SSL_LIB-NOTFOUND")
      message(FATAL_ERROR "Openssl library libssl.a not found")
   endif()

   find_library(OPENSSL_CRYPTO_LIB
               names ${OPENSSL_CRYPTO_LIB_NAME}
               HINTS ${OPENSSL_LIB_DIR})

   if(${OPENSSL_CRYPTO_LIB} STREQUAL "OPENSSL_CRYPTO_LIB-NOTFOUND")
      message(FATAL_ERROR "Openssl library libcrypto.a not found")
   endif()

   link_directories(${OPENSSL_LIB_DIR})
endif()

find_library(SDB_CLT_LIB
             names ${SDB_DRIVER_LIB_NAME}
             HINTS ${SDB_DRIVER_LIB_DIR})

if(${SDB_CLT_LIB} STREQUAL "SDB_CLT_LIB-NOTFOUND")
   message(FATAL_ERROR "SequoiaDB client dynamic library not found")
endif()



include_directories(${SDB_DRIVER_INCLUDE_DIR})
link_directories(${SDB_DRIVER_LIB_DIR})

#aux_source_directory('./' SOURCE_FILES)
add_executable(sdbvertool sdbVerTool.cpp)
if(OPENSSL_LIB_DIR)
   target_link_libraries(sdbvertool sdbcpp ssl crypto dl)
else()
   target_link_libraries(sdbvertool sdbcpp dl)
endif()

