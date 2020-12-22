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
cmake_minimum_required(VERSION 2.8.7)
# spark storage engine
if(CMAKE_BUILD_TYPE MATCHES "Debug")
  add_definitions(-DDEBUG)
endif()
add_definitions(-Wno-error)

include_directories(include)
aux_source_directory(handler HANDLER_FILES)
aux_source_directory(sparkwrapper SPK_FILES)
set(SPARK_SOURCES ${HANDLER_FILES} ${SPK_FILES})

include_directories(${ODBC_INCLUDE_DIR})
include_directories(include)

add_compile_options(-Wno-unused-variable -Wno-unused-function -Wno-write-strings -Wno-return-type)
set(SPK_LDFLAGS "-Wl,-rpath,${CMAKE_INSTALL_PREFIX}/lib")
MYSQL_ADD_PLUGIN(spark ${SPARK_SOURCES} 
                 STORAGE_ENGINE
                 MANDATORY
                 LINK_LIBRARIES
                 ${ODBC_LIBRARY})

set_target_properties(spark PROPERTIES LINK_FLAGS ${SPK_LDFLAGS})
install(FILES ${ODBC_LIBRARY} DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)