/*******************************************************************************

   Copyright (C) 2011-Present SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   
*******************************************************************************/
#ifndef SDB_LOCK__H
#define SDB_LOCK__H

#include <thr_mutex.h>
#include <thr_rwlock.h>

class Sdb_mutex {
  native_mutex_t m_mutex;

 public:
  Sdb_mutex() { native_mutex_init(&m_mutex, MY_MUTEX_INIT_FAST); }

  ~Sdb_mutex() { native_mutex_destroy(&m_mutex); }

  inline int lock() { return native_mutex_lock(&m_mutex); }

  inline int unlock() { return native_mutex_unlock(&m_mutex); }
};

class Sdb_mutex_guard {
  Sdb_mutex &m_mutex;

 public:
  Sdb_mutex_guard(Sdb_mutex &mutex) : m_mutex(mutex) { m_mutex.lock(); }

  ~Sdb_mutex_guard() { m_mutex.unlock(); }
};

class Sdb_rwlock {
  native_rw_lock_t rw_lock;

 public:
  Sdb_rwlock() { native_rw_init(&rw_lock); }

  ~Sdb_rwlock() { native_rw_destroy(&rw_lock); }

  inline int read_lock() { return native_rw_rdlock(&rw_lock); }

  inline int write_lock() { return native_rw_wrlock(&rw_lock); }

  inline int unlock() { return native_rw_unlock(&rw_lock); }
};

class Sdb_rwlock_read_guard {
  Sdb_rwlock &m_rwlock;

 public:
  Sdb_rwlock_read_guard(Sdb_rwlock &lock) : m_rwlock(lock) {
    m_rwlock.read_lock();
  }

  ~Sdb_rwlock_read_guard() { m_rwlock.unlock(); }
};

class Sdb_rwlock_write_guard {
  Sdb_rwlock &m_rwlock;

 public:
  Sdb_rwlock_write_guard(Sdb_rwlock &lock) : m_rwlock(lock) {
    m_rwlock.write_lock();
  }

  ~Sdb_rwlock_write_guard() { m_rwlock.unlock(); }
};

#endif /* SDB_LOCK__H */
