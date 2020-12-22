# Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51 Franklin
# St, Fifth Floor, Boston, MA  02110-1301 USA

# Usage: cmake -DSDB_BUILT_IN=ON -DWITH_SDB_DRIVER=/opt/sequoiadb
#
# [SDB_BUILT_IN]: compile as built-in plugin or module plugin.
# [WITH_SDB_DRIVER]: the path to SequoiaDB cpp driver, which must contain
# include/ and lib/libsdbcpp.so

if(CMAKE_BUILD_TYPE MATCHES "Debug")
  add_definitions(-DDEBUG)
endif()
add_definitions(-Wno-error)

if(SDB_PROJECT_TYPE STREQUAL "mysql")
  add_compile_options(-DIS_MYSQL)
else()
  add_compile_options(-DIS_MARIADB)
  add_definitions(-Wno-deprecated-declarations)
endif()

include_directories(include)
aux_source_directory(handler HANDLER_FILES)
aux_source_directory(sdbwrapper SDB_FILES)
aux_source_directory(server_ha SERVER_HA_FILES)
set(SEQUOIADB_SOURCES ${HANDLER_FILES} ${SDB_FILES})

set(WITH_SDB_DRIVER "" CACHE PATH "Path to SequoiaDB C++ driver")
set(SDB_DRIVER_PATH ${WITH_SDB_DRIVER})

find_path(SDB_INCLUDE_DIR NAMES client.hpp HINTS ${SDB_DRIVER_PATH}/include)

if(${SDB_INCLUDE_DIR} STREQUAL "SDB_INCLUDE_DIR-NOTFOUND")
  message(
    SEND_ERROR
      "Cannot find appropriate headers of SequoiaDB C++ driver from ${SDB_DRIVER_PATH}"
    )
endif()
message(STATUS "SDB_INCLUDE_DIR = ${SDB_INCLUDE_DIR}")
include_directories(${SDB_INCLUDE_DIR})

set(SDB_LDFLAGS "-Wl,-rpath,${CMAKE_INSTALL_PREFIX}/lib")

if(SDB_BUILT_IN)
  set(_SDB_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  set(CMAKE_FIND_LIBRARY_SUFFIXES "")
  # builtin storage engine use C++ dynamic driver
  find_library(SDB_DYNAMIC_LIBRARY
               NAMES libsdbcpp.so
               HINTS ${SDB_DRIVER_PATH}/lib)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_SDB_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
  if(${SDB_DYNAMIC_LIBRARY} STREQUAL "SDB_DYNAMIC_LIBRARY-NOTFOUND")
    message(SEND_ERROR "Cannot find libsdbcpp.so from ${SDB_DRIVER_PATH}/lib")
  endif()
  message(STATUS "Using SequoiaDB dynamic library: ${SDB_DYNAMIC_LIBRARY}")

  mysql_add_plugin(sequoiadb
                   ${SEQUOIADB_SOURCES}
                   STORAGE_ENGINE
                   MANDATORY
                   RECOMPILE_FOR_EMBEDDED
                   LINK_LIBRARIES
                   ${SDB_DYNAMIC_LIBRARY})

  add_compile_options(-Wno-unused-variable -Wno-unused-function -Wno-write-strings -Wno-return-type)
  mysql_add_plugin(server_ha
                   ${SERVER_HA_FILES}
                   MANDATORY
                   LINK_LIBRARIES
                   ${SDB_DYNAMIC_LIBRARY}
                   RECOMPILE_FOR_EMBEDDED)
  string(FIND "${WITH_MYSQLD_LDFLAGS}" "${SDB_LDFLAGS}" POS)
  if(${POS} EQUAL -1)
    set(WITH_MYSQLD_LDFLAGS "${WITH_MYSQLD_LDFLAGS} ${SDB_LDFLAGS}"
        CACHE STRING "Additional linker flags for mysqld"
        FORCE)
  endif()

  install(FILES ${SDB_DYNAMIC_LIBRARY} DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)
else()
  set(_SDB_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  set(CMAKE_FIND_LIBRARY_SUFFIXES "")
  # use C++ static driver
  find_library(SDB_STATIC_LIBRARY
               NAMES libstaticsdbcpp.a
               HINTS ${SDB_DRIVER_PATH}/lib)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_SDB_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
  if(${SDB_STATIC_LIBRARY} STREQUAL "SDB_STATIC_LIBRARY-NOTFOUND")
    message(
      SEND_ERROR "Cannot find libstaticsdbcpp.a from ${SDB_DRIVER_PATH}/lib")
  endif()
  message(STATUS "Using SequoiaDB static library: ${SDB_STATIC_LIBRARY}")

  mysql_add_plugin(sequoiadb
                   ${SEQUOIADB_SOURCES}
                   STORAGE_ENGINE
                   MODULE_ONLY
                   LINK_LIBRARIES
                   ${SDB_STATIC_LIBRARY})
  set_target_properties(sequoiadb PROPERTIES LINK_FLAGS ${SDB_LDFLAGS})
endif()

if(WITH_BOOST)
  # set openssl dir
  set(OPENSSL_DIR "${SQL_SOURCE_ROOT}/../../thirdparty/openssl-1.1.1g")
  set(OPENSSL_INCLUDE_DIR "${OPENSSL_DIR}/include")
  set(OPENSSL_LIB_DIR "${OPENSSL_DIR}/install_path/lib")

  # build ha_inst_group_init
  set(HA_TOOLS_FLAGS "-Wno-unused-value -Wno-missing-field-initializers -Wno-deprecated-declarations")

  set(HA_TOOLS_INCLUDE "${WITH_BOOST};${SDB_DRIVER_PATH}/include;${CMAKE_CURRENT_SOURCE_DIR}/include;${OPENSSL_INCLUDE_DIR}")
  link_directories(${SDB_DRIVER_PATH}/lib ${OPENSSL_LIB_DIR})
  add_executable(ha_inst_group_init tools/ha_inst_group_init.cc tools/ha_tool_utils.cc)
  set_target_properties(ha_inst_group_init PROPERTIES COMPILE_FLAGS ${HA_TOOLS_FLAGS})
  set_target_properties(ha_inst_group_init PROPERTIES INCLUDE_DIRECTORIES "${HA_TOOLS_INCLUDE}")
  target_link_libraries(ha_inst_group_init sdbcpp ssl crypto dl pthread)

  # build ha_inst_group_clear
  add_executable(ha_inst_group_clear tools/ha_inst_group_clear.cc tools/ha_tool_utils.cc)
  set_target_properties(ha_inst_group_clear PROPERTIES COMPILE_FLAGS ${HA_TOOLS_FLAGS})
  set_target_properties(ha_inst_group_clear PROPERTIES INCLUDE_DIRECTORIES "${HA_TOOLS_INCLUDE}")
  target_link_libraries(ha_inst_group_clear sdbcpp ssl crypto dl pthread)

  # build ha_inst_group_list
  add_executable(ha_inst_group_list tools/ha_inst_group_list.cc tools/ha_tool_utils.cc)
  set_target_properties(ha_inst_group_list PROPERTIES COMPILE_FLAGS ${HA_TOOLS_FLAGS})
  set_target_properties(ha_inst_group_list PROPERTIES INCLUDE_DIRECTORIES "${HA_TOOLS_INCLUDE}")
  target_link_libraries(ha_inst_group_list sdbcpp ssl crypto dl pthread)

  install(TARGETS ha_inst_group_init ha_inst_group_clear ha_inst_group_list
          RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
endif()

# apply patches for connector
set(SERVER_HA_PATCHES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/patches")
if(EXISTS "${SERVER_HA_PATCHES_DIR}")
  if(SDB_PROJECT_TYPE STREQUAL "mariadb")
    set(PATCH_FILE "${SERVER_HA_PATCHES_DIR}/ha_mariadb_${MARIADB}.patch")
    set(DUMP_PATCH_FILE "${SERVER_HA_PATCHES_DIR}/mariadb_mysqldump_${MARIADB}.patch")
    set(SEQ_PATCH_FILE "${SERVER_HA_PATCHES_DIR}/seq_mariadb_${MARIADB}.patch")
    set(MROONGA_PATCH_FILE "${SERVER_HA_PATCHES_DIR}/mroonga_mariadb_${MARIADB}.patch")
  else()
    set(PATCH_FILE "${SERVER_HA_PATCHES_DIR}/ha_mysql_${MYSQL}.patch")
    set(DUMP_PATCH_FILE "${SERVER_HA_PATCHES_DIR}/mysql_mysqldump_${MYSQL}.patch")
  endif()

  # apply HA patch file for 'sql/*'
  if (EXISTS "${PATCH_FILE}")
    set(APPLY_PATCH_TARGET "${SQL_SOURCE_ROOT}/sql")
    message(STATUS "Apply patch ${PATCH_FILE} on ${APPLY_PATCH_TARGET} directory")
    execute_process(COMMAND cmake -E chdir "${SQL_SOURCE_ROOT}/sql" patch -s -N -i ${PATCH_FILE})
  else()
    message(FATAL_ERROR "Patch file ${PATCH_FILE} doesn't exist")
  endif()

  # apply mysqldump patch file
  if (EXISTS "${DUMP_PATCH_FILE}")
    set(APPLY_PATCH_TARGET "${SQL_SOURCE_ROOT}/client/mysqldump.c")
    message(STATUS "Apply patch ${DUMP_PATCH_FILE} on ${APPLY_PATCH_TARGET} for 'server_ha' module")
    execute_process(COMMAND cmake -E chdir "${SQL_SOURCE_ROOT}/client" patch -s -N -i ${DUMP_PATCH_FILE})
  else()
    message(FATAL_ERROR "Patch file ${DUMP_PATCH_FILE} doesn't exist")
  endif()

  # apply sequence patch file for 'include/*'
  if (SDB_PROJECT_TYPE STREQUAL "mariadb")
    if (EXISTS "${SEQ_PATCH_FILE}")
      set(APPLY_PATCH_TARGET "${SQL_SOURCE_ROOT}/include")
      message(STATUS "Apply patch ${SEQ_PATCH_FILE} on ${APPLY_PATCH_TARGET} directory for sequence")
      execute_process(COMMAND cmake -E chdir "${SQL_SOURCE_ROOT}/include" patch -s -N -i ${SEQ_PATCH_FILE})
    else()
      message(FATAL_ERROR "Patch file ${SEQ_PATCH_FILE} doesn't exist")
    endif()
  endif()

  # apply sequence patch file for 'storage/mroonga/*'
  if (SDB_PROJECT_TYPE STREQUAL "mariadb")
    if (EXISTS "${MROONGA_PATCH_FILE}")
      set(APPLY_PATCH_TARGET "${SQL_SOURCE_ROOT}/storage/mroonga")
      message(STATUS "Apply patch ${MROONGA_PATCH_FILE} on ${APPLY_PATCH_TARGET} directory for sequence")
      execute_process(COMMAND cmake -E chdir "${SQL_SOURCE_ROOT}/storage/mroonga" patch -s -N -i ${MROONGA_PATCH_FILE})
    else()
      message(FATAL_ERROR "Patch file ${SEQ_PATCH_FILE} doesn't exist")
    endif()
  endif()

else()
  message(FATAL_ERROR "Can't apply 'server_ha' patch, patch directory 'patches' doesn't exist")
endif()

# spark storage engine
SET(SPARK_SOURCES sparkwrapper/ha_spark.cc
     sparkwrapper/ha_spark_vars.cc 
     sparkwrapper/spark_conn.cc
     sparkwrapper/ha_spark_log.cc )
include_directories(${ODBC_INCLUDE_DIR})
link_directories("/usr/lib/x86_64-linux-gnu")
MYSQL_ADD_PLUGIN(spark ${SPARK_SOURCES} 
                 STORAGE_ENGINE MODULE_ONLY
                 LINK_LIBRARIES
                 ${ODBC_LIBRARY})