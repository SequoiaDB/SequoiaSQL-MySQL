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

cmake_minimum_required(VERSION 2.8.7)

option(ENTERPRISE "Build enterprise edition" OFF)
option(SDB_BUILT_IN "Build the sequoiadb plugin into mysqld" ON)
option(PACK_TEST "Set to true if you want to include testcases in the run
                  package" ON)

# Find out with target(s) to be build.
if(MYSQL AND MARIADB)
   message(FATAL_ERROR "Can only build one type at the same time")
elseif(MARIADB)
   set(SDB_PROJECT_TYPE "mariadb" CACHE STRING "Project type is MariaDB")
   set(PROJECT_NAME "SequoiaSQL-MariaDB")
   set(SQL_VERSION "${MARIADB}")
else()
   set(SDB_PROJECT_TYPE "mysql" CACHE STRING "Project type is MySQL")
   if(NOT MYSQL)
      set(MYSQL "5.7.25")
   endif()
   set(PROJECT_NAME "SequoiaSQL-MySQL")
   set(SQL_VERSION "${MYSQL}")
endif()

#if(NOT (${CMAKE_VERSION} VERSION_LESS "3.0"))
#   cmake_policy(SET CMP0048 OLD)
#endif()

project(${PROJECT_NAME})

set(VERSION_FILE ${CMAKE_SOURCE_DIR}/version)
execute_process(
   COMMAND bash -c "awk -F. '{print $1}' ${VERSION_FILE}"
   OUTPUT_VARIABLE PROJECT_VERSION_MAJOR
)
string(STRIP ${PROJECT_VERSION_MAJOR} PROJECT_VERSION_MAJOR)
execute_process(
   COMMAND bash -c "awk -F. '{print $2}' ${VERSION_FILE}"
   OUTPUT_VARIABLE PROJECT_VERSION_MINOR
)
string(STRIP ${PROJECT_VERSION_MINOR} PROJECT_VERSION_MINOR)
execute_process(
   COMMAND bash -c "grep -o '\\.' ${VERSION_FILE}"
   COMMAND bash -c "wc -l"
   OUTPUT_VARIABLE DOT_NUMBER
)

# Check if the version number contains path version.
if(DOT_NUMBER EQUAL 2)
   execute_process(
      COMMAND bash -c "awk -F. '{print $3}' ${VERSION_FILE}"
      OUTPUT_VARIABLE PROJECT_VERSION_PATCH
   )
   string(STRIP ${PROJECT_VERSION_PATCH} PROJECT_VERSION_PATCH)
   set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}")
else()
   unset(PROJECT_VERSION_PATCH)
   set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}")
endif()

if(SDB_PROJECT_TYPE STREQUAL "mariadb")
   add_definitions(-DOPENSSL_ROOT_DIR="${CMAKE_SOURCE_DIR}/thirdparty/openssl-1.0.1c")
endif()

# Trigger the sdb version tool building, and use it to get the version.
function(get_sdb_version version git_version)
   set(SDB_VERTOOL_DIR ${CMAKE_SOURCE_DIR}/misc/versiontool)
   set(SDB_VERTOOL ${SDB_VERTOOL_DIR}/sdbvertool)

   # Build the sdb version tool in misc.
   if(IS_ABSOLUTE ${WITH_SDB_DRIVER})
      set(SDB_DRIVER_DIR "${WITH_SDB_DRIVER}")
   else()
      set(SDB_DRIVER_DIR "${CMAKE_BINARY_DIR}/${WITH_SDB_DRIVER}")
   endif()

   execute_process(
      COMMAND bash -c "cmake . -DSDB_DRIVER_DIR=${SDB_DRIVER_DIR}"
      WORKING_DIRECTORY "${SDB_VERTOOL_DIR}"
   )

   execute_process(
      COMMAND bash -c "make"
      WORKING_DIRECTORY "${SDB_VERTOOL_DIR}"
   )

   # Execute the sdb version tool to get the version.
   execute_process(
      COMMAND bash -c "${SDB_VERTOOL}"
      COMMAND bash -c "grep Version"
      COMMAND bash -c "awk '{print $2}'"
      OUTPUT_VARIABLE SDB_VERSION
   )

   execute_process(
      COMMAND bash -c "${SDB_VERTOOL}"
      COMMAND bash -c "grep Git"
      COMMAND bash -c "awk '{print $3}'"
      OUTPUT_VARIABLE SDB_GIT_VERSION
   )

   # The original value contains a '\n' at the end. Remove it.
   string(STRIP ${SDB_VERSION} SDB_VERSION)
   string(STRIP ${SDB_GIT_VERSION} SDB_GIT_VERSION)

   set(${version} ${SDB_VERSION} PARENT_SCOPE)
   set(${git_version} ${SDB_GIT_VERSION} PARENT_SCOPE)
endfunction()

function(get_my_version my_version length)
   if(${length} EQUAL 0)
      set(GIT_LOG_CMD "git log -1 --format=%H")
   else()
      set(GIT_LOG_CMD "git log -1 --format=%h --abbrev=${length}")
   endif()
   execute_process(
      COMMAND bash -c "${GIT_LOG_CMD}"
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_VARIABLE GIT_VERSION
   )

   string(STRIP ${GIT_VERSION} GIT_VERSION)
   set(${my_version} ${GIT_VERSION} PARENT_SCOPE)
endfunction()

# Generate the project version file, which contains version of this project and
# the SequoiaDB driver.
function(gen_version_file)
   set(SDB_PLUGIN_VERSION "")
   get_my_version(SDB_PLUGIN_VERSION 0)
   message(STATUS "SDB plugin version: ${SDB_PLUGIN_VERSION}")

   set(SDB_VERSION "")
   set(SDB_GIT_VERSION "")
   get_sdb_version(SDB_VERSION SDB_GIT_VERSION)

   string(TIMESTAMP BUILDDATE "%Y-%m-%d-%H.%M.%S")
   set(BUILD_TYPE_STR "")
   if(ENTERPRISE)
      string(APPEND BUILD_TYPE_STR "Enterprise")
   endif()
   if(CMAKE_BUILD_TYPE MATCHES "Debug")
      if(BUILD_TYPE_STR STREQUAL "Enterprise")
         string(APPEND BUILD_TYPE_STR "-Debug")
      else()
         string(APPEND BUILD_TYPE_STR "Debug")
      endif()
   else()
      SET(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING
          "Type of build, options are: Debug RelWithDebInfo" FORCE)
   endif()
   if(NOT (BUILD_TYPE_STR STREQUAL ""))
      set(BUILD_TYPE_STR " (${BUILD_TYPE_STR})")
   endif()

   string(CONCAT FULL_VERSION "${PROJECT_NAME} version: ${PROJECT_VERSION}\n"
                              "Git version: ${SDB_PLUGIN_VERSION}\n"
                              "${BUILDDATE}" "${BUILD_TYPE_STR}\n\n"
                              "SDB driver: ${SDB_VERSION}("
                              "${SDB_GIT_VERSION})\n")
   file(WRITE ${CMAKE_BINARY_DIR}/version.info ${FULL_VERSION})
endfunction()

function(prepare_sql_source sql_source_root sql_name)
   if(NOT EXISTS ${sql_source_root}/CMakeLists.txt)
      file(READ ${sql_source_root}/url.txt SOURCE_URL)
      message(STATUS "SQL source code not exist. Download it from ${SOURCE_URL}")
      set(PACKAGE_NAME "${sql_name}.tar.gz")
      set(DOWNLOAD_TARGET "${sql_source_root}/${PACKAGE_NAME}")
      file(DOWNLOAD ${SOURCE_URL} ${DOWNLOAD_TARGET})
      execute_process(
         COMMAND ${CMAKE_COMMAND} -E tar zxvf "${DOWNLOAD_TARGET}"
         WORKING_DIRECTORY "${sql_source_root}"
         RESULT_VARIABLE tar_result
      )
      set(EXTRACT_NAME "${SDB_PROJECT_TYPE}-v${SQL_VERSION}")
      execute_process(
         COMMAND bash -c "mv ${EXTRACT_NAME}/* ./"
         WORKING_DIRECTORY "${sql_source_root}"
         RESULT_VARIABLE tar_result
      )
   endif()
endfunction()

set(SQL_SOURCE_DIR_NAME "${SDB_PROJECT_TYPE}-${SQL_VERSION}")
set(SQL_SOURCE_ROOT "${CMAKE_SOURCE_DIR}/sql-src/${SQL_SOURCE_DIR_NAME}")

if(NOT EXISTS ${SQL_SOURCE_ROOT})
   message(FATAL_ERROR "Source code path [${SQL_SOURCE_ROOT}] dose not exist")
endif()

prepare_sql_source(${SQL_SOURCE_ROOT} ${SQL_SOURCE_DIR_NAME})

if(NOT WITH_SDB_DRIVER)
   message(FATAL_ERROR "SequoiaDB driver path not set")
endif()

# If WITH_BOOST is not set, use the default
if(NOT WITH_BOOST)
   set(BOOST_VERSION "1_59_0")
   set(WITH_BOOST "${CMAKE_SOURCE_DIR}/thirdparty/boost_${BOOST_VERSION}")
endif()
include_directories(SYSTEM ${WITH_BOOST})

if(NOT SDB_BUILT_IN)
   set(SDB_BUILD_IN ${SDB_BUILD_IN} CACHE BOOL
       "Build the sequoiadb plugin into mysqld")
endif()

if(NOT MYSQL_DATADIR)
   set(MYSQL_DATADIR "/opt/sequoiasql/mysql/data")
endif()

if(ENTERPRISE)
   set(CMAKE_CXX_FLAGS "-DSDB_ENTERPRISE=TRUE")
endif()

set(SDB_PLUGIN_VERSION "")
get_my_version(SDB_PLUGIN_VERSION 7)
message(STATUS "SDB plugin version: ${SDB_PLUGIN_VERSION}")

set(SDB_VERSION "")
set(SDB_GIT_VERSION "")
get_sdb_version(SDB_VERSION SDB_GIT_VERSION)

add_definitions(-DSDB_PLUGIN_VERSION="${SDB_PLUGIN_VERSION}")
add_definitions(-DSDB_DRIVER_VERSION="${SDB_VERSION}")

if(PACKET_TEST)
   message(STATUS "Packet testcases")
endif()

# Preprocessing
set(SQL_STORAGE_DIR "${SQL_SOURCE_ROOT}/storage")
set(SQL_CONNECTOR_DIR "${CMAKE_SOURCE_DIR}/src")

# Replace the original mysql-test with the new one.
set(MYSQL_TEST_DIR "${SQL_SOURCE_ROOT}/mysql-test")
set(MYSQL_TEST_ORIG "${SQL_SOURCE_ROOT}/mysql-test-orig")

execute_process(
   COMMAND bash -c "file ${MYSQL_TEST_DIR}"
   COMMAND bash -c "awk '{print $2}'"
   OUTPUT_VARIABLE TEST_DIR_TYPE
)

# Remove the native mysql testcases, and create a symbol link pointing to ours.
string(STRIP ${TEST_DIR_TYPE} TEST_DIR_TYPE)
if(TEST_DIR_TYPE STREQUAL "directory")
   file(REMOVE_RECURSE ${MYSQL_TEST_DIR})
   execute_process(
      COMMAND bash -c "ln -sf ${CMAKE_SOURCE_DIR}/testcase/${SQL_SOURCE_DIR_NAME} ${SQL_SOURCE_ROOT}/mysql-test"
   )
endif()

set(updateCmd "find ${SQL_SOURCE_ROOT} -type f |
               xargs grep -l CMAKE_SOURCE_DIR 2>/dev/null |
               xargs sed -i 's/CMAKE_SOURCE_DIR/SQL_SOURCE_ROOT/g' 2>/dev/null")
execute_process(COMMAND bash -c ${updateCmd})

set(SDB_PLUGIN_DIR "${SQL_STORAGE_DIR}/sequoiadb")
if(EXISTS ${SDB_PLUGIN_DIR})
   message(STATUS "Directory ${SDB_PLUGIN_DIR} exists. Ready to remove")
   file(REMOVE ${SDB_PLUGIN_DIR})
else()
   message(STATUS "Directory ${SDB_PLUGIN_DIR} dose not exist")
endif()

# Generate the version.info file
gen_version_file()

set(mapSrcCmd "ln -sf ${SQL_CONNECTOR_DIR} ${SQL_STORAGE_DIR}/sequoiadb")
execute_process(COMMAND bash -c ${mapSrcCmd})

set(SQL_OUTPUT_ROOT "${CMAKE_BINARY_DIR}/${SQL_SOURCE_DIR_NAME}"
    CACHE STRING INTERNAL)
set(updateCmd "find ${SQL_SOURCE_ROOT} -type f |
               xargs grep -l CMAKE_BINARY_DIR 2>/dev/null |
               xargs sed -i 's#\${CMAKE_BINARY_DIR}#${SQL_OUTPUT_ROOT}#g'
               2>/dev/null")
execute_process(COMMAND bash -c ${updateCmd})

set(AUTOGEN_include_DIR "${CMAKE_CURRENT_BINARY_DIR}/include")
include_directories(${AUTOGEN_include_DIR})

option(WITH_DEBUG "USE DBUG/SAFEMUTEX" OFF)

if(WITH_DEBUG)
   message(STATUS "Build debug")
else()
   message(STATUS "Build release")
endif()

if(SDB_BUILT_IN)
   set(BUILT_IN 1)
else()
   set(BUILT_IN 0)
endif()

set(TARGET_SOURCE_DIR "${CMAKE_SOURCE_DIR}/sql-src/${target}")
add_subdirectory(${SQL_SOURCE_ROOT} ${CMAKE_BINARY_DIR}/${SQL_SOURCE_DIR_NAME})

# Generate the configuration file
execute_process(
   COMMAND python ${CMAKE_SOURCE_DIR}/tools/sdb_doc_generator.py 
           -t ${SDB_PROJECT_TYPE} -o ${CMAKE_BINARY_DIR} -b ${BUILT_IN}
)

install(FILES "${CMAKE_BINARY_DIR}/sdb_doc.cnf"
        DESTINATION "conf/sample/"
        RENAME "sample.cnf")

# Extra install options.
set(PROJECT_PACKAGE_DIR "${CMAKE_SOURCE_DIR}/package")
set(PROJECT_PACKAGE_SUB_DIR "${PROJECT_PACKAGE_DIR}/${SDB_PROJECT_TYPE}")
install(PROGRAMS "${PROJECT_PACKAGE_SUB_DIR}/sdb_sql_ctl"
        DESTINATION "bin")
install(PROGRAMS "${PROJECT_PACKAGE_DIR}/common/compatible.sh"
        DESTINATION "./")
install(PROGRAMS "${PROJECT_PACKAGE_DIR}/common/preUninstall.sh"
        DESTINATION "./")
install(DIRECTORY "${PROJECT_PACKAGE_SUB_DIR}/license"
        DESTINATION "./")
install(DIRECTORY "${CMAKE_SOURCE_DIR}/tools/lib"
        DESTINATION "tools")
install(DIRECTORY "${CMAKE_SOURCE_DIR}/tools/metaSync"
        DESTINATION "tools")
install(FILES "${CMAKE_BINARY_DIR}/version.info"
        DESTINATION "./")

include("${CMAKE_SOURCE_DIR}/package/cmakecpackage.cmake")


if(PACK_TEST)
   set(CLEAN_TEST_CMD ":" CACHE STRING "Take the mysql-test when packing")
else()
   set(CLEAN_TEST_CMD "rm ${CMAKE_INSTALL_PREFIX}/mysql-test" CACHE STRING "Remove the mysql-test before packing")
endif()

if(ENTERPRISE)
   set(EDITION_VAR "-enterprise")
else()
   set(EDITION_VAR "")
endif()
set(BUILD_COMMAND "builder build ${PROJECT_PACKAGE_SUB_DIR}/sequoiamysql_server.xml --setvars project.version=${PROJECT_VERSION} --setvars platform=linux_x86_64 --setvars edition=${EDITION_VAR} --setvars project.outputDirectory=${CMAKE_BINARY_DIR}")

if(PACK_TEST)
   add_custom_target(
      runpackage
      COMMAND bash -c "cp ${PROJECT_PACKAGE_SUB_DIR}/*.sys* ${CMAKE_INSTALL_PREFIX}/bin"
      COMMAND bash -c "ln -sf ${CMAKE_INSTALL_PREFIX} ${PROJECT_PACKAGE_SUB_DIR}/${SDB_PROJECT_TYPE}"
      COMMAND bash -c ${BUILD_COMMAND}
      comment "Building .run package...")
else()
   add_custom_target(
      runpackage
      COMMAND bash -c "rm -rf ${CMAKE_INSTALL_PREFIX}/mysql-test"
      COMMAND bash -c "cp ${PROJECT_PACKAGE_SUB_DIR}/*.sys* ${CMAKE_INSTALL_PREFIX}/bin"
      COMMAND bash -c "ln -sf ${CMAKE_INSTALL_PREFIX} ${PROJECT_PACKAGE_SUB_DIR}/${SDB_PROJECT_TYPE}"
      COMMAND bash -c ${BUILD_COMMAND}
      comment "Building .run package...")
endif()

add_custom_target(testcase
   COMMAND bash -c "ln -sf ${CMAKE_INSTALL_PREFIX}/lib/libsdbcpp.so libsdbcpp.so"
   COMMAND bash -c "./mysql-test-run.pl --suite=main,json --big-test --force --max-test-fail=0 --retry=1 --retry-failure=1 --parallel=4 --xml-report=${CMAKE_BINARY_DIR}/mysql_test_report.xml"
   WORKING_DIRECTORY "${CMAKE_INSTALL_PREFIX}/mysql-test"
   DEPENDS ${CMAKE_INSTALL_PREFIX}
   COMMENT "Run testcases...")

add_custom_target(testpackage
   COMMAND bash -c "tar zcvf ${CMAKE_BINARY_DIR}/${SQL_SOURCE_DIR_NAME}-test.tar.gz *"
   WORKING_DIRECTORY "${CMAKE_INSTALL_PREFIX}/mysql-test"
   DEPENDS ${CMAKE_INSTALL_PREFIX}
   COMMENT "Make testcase package..."
)
