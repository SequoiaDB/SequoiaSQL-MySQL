# Copyright (c) 2006, 2016, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

IF (SDB_VER)
  ADD_DEFINITIONS(-DSDB_VER=\"${SDB_VER}\")
ENDIF()
IF (CMAKE_BUILD_TYPE MATCHES "Debug")
  ADD_DEFINITIONS(-DDEBUG)
ENDIF()
ADD_DEFINITIONS(-Wno-error)

SET(SEQUOIADB_SOURCES
ha_sdb.cc
sdb_conf.cc
sdb_util.cc
sdb_conn.cc
sdb_condition.cc
sdb_item.cc
sdb_cl.cc
sdb_adaptor.cc
sdb_err_code.cc
sdb_idx.cc)

INCLUDE_DIRECTORIES(
./sequoiadb
./sequoiadb/include )

SET(SDB_DRIVER_PATH ${CMAKE_CURRENT_SOURCE_DIR}/sequoiadb/lib/libstaticsdbcpp.a)

SET(SDB_BUILT_IN OFF CACHE BOOL "whether build SequoiaDB storage engine in mysqld")
MARK_AS_ADVANCED(SDB_BUILT_IN)
IF(SDB_BUILT_IN)
  MYSQL_ADD_PLUGIN(sequoiadb ${SEQUOIADB_SOURCES} STORAGE_ENGINE MANDATORY RECOMPILE_FOR_EMBEDDED 
      LINK_LIBRARIES ${SDB_DRIVER_PATH})
ELSE()
  MYSQL_ADD_PLUGIN(sequoiadb ${SEQUOIADB_SOURCES} STORAGE_ENGINE MODULE_ONLY
      LINK_LIBRARIES ${SDB_DRIVER_PATH})
ENDIF()
