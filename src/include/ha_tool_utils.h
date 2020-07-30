/* Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef HA_TOOLS_UTIL__H
#define HA_TOOLS_UTIL__H

#include <string>
#include <openssl/bio.h>
#include <client.hpp>
#include "server_ha_def.h"
#include "ha_sdb_def.h"

#define HA_TOOL_RC_CHECK(cond, rc, fmt, ...) \
  do {                                       \
    if ((cond)) {                            \
      fprintf(stderr, fmt, ##__VA_ARGS__);   \
      fprintf(stderr, "\n");                 \
      return rc;                             \
    }                                        \
  } while (0)

typedef unsigned char uchar;

// struct corresponding to 'HAInstGroupConfig' table
typedef struct st_inst_group_config {
  // corresponding 'authentication_string' field for mysql.user table
  std::string auth_str;
  // base64-formatted cipher password
  std::string cipher_password;
  // password encrypted in MD5 alogrithm
  std::string md5_password;
  // initial vector for aes algorithm
  std::string iv;
  // instance group user name
  std::string user;
  int explicit_defaults_ts;
} ha_inst_group_config_cl;

// use to store command line arguments
typedef struct st_args {
  // instance group name
  std::string inst_group_name;
  // force to delete instance group without confirm
  bool force;
  // instance group key
  std::string key;
  // user used to write instance group info into sequoiadb
  std::string user;
  std::string password;
  // coord service address
  std::string host;
  // host parsed from host
  std::string hostname;
  // coord service port parsed from hosts
  uint port;
  // token and file_name used to support 'cipher file', refer sdbpasswd
  std::string token;
  std::string file_name;

  // check if user input 'user', 'password', 'verbose' argument
  bool is_user_set, is_password_set, verbose;

  // inst_id used to delete instance information
  int inst_id;
  // check if inst_id is set
  bool is_inst_id_set;
  std::string inst_hostname;
  uint inst_port;
  std::string inst_host;
} ha_tool_args;

enum HA_EVP_MD_TYPE { HA_EVP_MD5, HA_EVP_SHA1, HA_EVP_SHA256 };
enum HA_ARGP_OPTION_KEY {
  HA_KEY_HOST = 10000,
  HA_KEY_KEY,
  HA_KEY_FILE,
  HA_KEY_INST_ID,
  HA_KEY_INST_HOST,
  HA_KEY_VERBOSE,
  HA_KEY_FORCE,
  HA_KEY_HELP,
  HA_KEY_USAGE
};

namespace {
struct BIOFreeAll {
  void operator()(BIO *p) { BIO_free_all(p); }
};
}  // namespace

static int getch();

std::string ha_get_password(const char *prompt, bool show_asterisk);

std::string ha_base64_encode(const std::vector<uchar> &binary);

std::vector<uchar> ha_base64_decode(const char *encoded);

int ha_random_string(std::string &out, uint len);

int ha_aes_128_cbc_encrypt(const std::string &str, uchar *cipher,
                           const uchar *md5_key, const uchar *md5_iv);

int ha_evp_digest(const std::string &str, uchar digest[], HA_EVP_MD_TYPE type);

int ha_create_mysql_auth_string(const std::string &passwd, std::string &out);

int ha_decrypt_password(const std::string &cipher, const std::string &token,
                        std::string &password);

void ha_extract_mix_cipher_string(const std::string &mix_cipher,
                                  std::string &passwd_cipher,
                                  std::string &rand_array);

int ha_evp_des_decrypt(const uchar *cipher, int len, uchar *out, uchar *key);

int ha_generate_des_key(uchar key[], const std::string &str);

int ha_parse_password(const std::string &user, const std::string &token,
                      const std::string &file_name, std::string &password);

void ha_init_default_args(st_args &cmd_args);

const std::string ha_error_string(int error_code);

const char *ha_sdb_error_string(sdbclient::sdb &conn, int rc);

int ha_parse_host(const std::string &host, std::string &hostname, uint &port);

int ha_init_sequoiadb_connection(sdbclient::sdb &conn, ha_tool_args &cmd_args);
#endif
