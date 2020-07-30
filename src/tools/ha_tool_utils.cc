#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <openssl/rand.h>
#include <openssl/err.h>
#include <boost/algorithm/hex.hpp>

#include <termios.h>
#include <unistd.h>
#include <string>
#include <memory>
#include <stdexcept>
#include <exception>
#include <iostream>
#include <assert.h>
#include <fstream>

#include "ha_tool_utils.h"

using namespace std;
int getch() {
  int ch;
  struct termios t_old, t_new;

  tcgetattr(STDIN_FILENO, &t_old);
  t_new = t_old;
  t_new.c_lflag &= ~(ICANON | ECHO);
  tcsetattr(STDIN_FILENO, TCSANOW, &t_new);

  ch = getchar();
  tcsetattr(STDIN_FILENO, TCSANOW, &t_old);
  return ch;
}

// get password from command line in invisible way
string ha_get_password(const char *prompt, bool show_asterisk = true) {
  const char BACKSPACE = 127;
  const char RETURN = 10;

  string password;
  uchar ch = 0;
  cout << prompt;

  while ((ch = getch()) != RETURN) {
    if (ch == BACKSPACE) {
      if (password.length() != 0) {
        if (show_asterisk)
          cout << "\b \b";
        password.resize(password.length() - 1);
      }
    } else {
      password += ch;
      if (show_asterisk)
        cout << '*';
    }
  }
  cout << endl;
  return password;
}

// encode binary array into base64-formatted string
std::string ha_base64_encode(const std::vector<uchar> &binary) {
  std::unique_ptr<BIO, BIOFreeAll> b64(BIO_new(BIO_f_base64()));
  BIO_set_flags(b64.get(), BIO_FLAGS_BASE64_NO_NL);
  BIO *sink = BIO_new(BIO_s_mem());
  BIO_push(b64.get(), sink);
  BIO_write(b64.get(), binary.data(), binary.size());
  BIO_flush(b64.get());
  const char *encoded;
  const long len = BIO_get_mem_data(sink, &encoded);
  return std::string(encoded, len);
}

// Assumes no newlines or extra characters in encoded string
std::vector<uchar> ha_base64_ecode(const char *encoded) {
  std::unique_ptr<BIO, BIOFreeAll> b64(BIO_new(BIO_f_base64()));
  BIO_set_flags(b64.get(), BIO_FLAGS_BASE64_NO_NL);
  BIO *source = BIO_new_mem_buf(encoded, -1);  // read-only source
  BIO_push(b64.get(), source);
  const int maxlen = strlen(encoded) / 4 * 3 + 1;
  std::vector<uchar> decoded(maxlen);
  const int len = BIO_read(b64.get(), decoded.data(), maxlen);
  decoded.resize(len);
  return decoded;
}

// generate a random string with length 'len'
int ha_random_string(string &out, uint len) {
  uint half_len = (len + 1) / 2;
  int evp_rc = 0;
  if (half_len > HA_MAX_RANDOM_STR_LEN) {
    return HA_ERR_RAND_STR_TOO_LONG;
  }

  uchar buf[HA_MAX_RANDOM_STR_LEN] = {0};
  evp_rc = RAND_bytes(buf, half_len);
  if (!evp_rc) {
    return ERR_get_error();
  }

  boost::algorithm::hex(buf, buf + half_len, std::back_inserter(out));
  assert(out.length() >= len);
  out = out.substr(0, len);
  return HA_ERR_OK;
}

// calculate digest for a string
int ha_evp_digest(const string &str, uchar digest[], HA_EVP_MD_TYPE type) {
  int evp_rc = 0;
  const EVP_MD *evp_md = NULL;
  switch (type) {
    case HA_EVP_MD5:
      evp_md = EVP_md5();
      break;
    case HA_EVP_SHA1:
      evp_md = EVP_sha1();
      break;
    case HA_EVP_SHA256:
      evp_md = EVP_sha256();
      break;
    default:
      break;
  }

  evp_rc = EVP_Digest(str.c_str(), str.length(), digest, NULL, evp_md, NULL);
  if (!evp_rc) {
    return ERR_get_error();
  }
  return HA_ERR_OK;
}

int ha_aes_128_cbc_encrypt(const string &str, uchar *cipher, const uchar *key,
                           const uchar *iv) {
  assert(cipher != NULL);
  assert(key != NULL);
  assert(iv != NULL);

  int outlen = 0;
  EVP_CIPHER_CTX *evp_ctx;
  evp_ctx = EVP_CIPHER_CTX_new();
  EVP_CipherInit_ex(evp_ctx, EVP_aes_128_cbc(), NULL, key, iv, 1);

  EVP_CipherUpdate(evp_ctx, cipher, &outlen, (uchar *)str.c_str(),
                   str.length());
  EVP_CipherFinal(evp_ctx, cipher + outlen, &outlen);
  EVP_CIPHER_CTX_free(evp_ctx);

  return HA_ERR_OK;
}

// create 'AuthString' field value for 'HAUser'
int ha_create_mysql_auth_string(const string &passwd, string &out) {
  std::string auth_str;
  uchar sha1[HA_SHA1_BYTE_LEN] = {0};
  uchar sha2[HA_SHA1_BYTE_LEN] = {0};
  int evp_rc = 0;

  uchar *str = (uchar *)passwd.c_str();
  evp_rc = EVP_Digest(str, passwd.length(), sha1, NULL, EVP_sha1(), NULL);
  if (!evp_rc) {
    evp_rc = ERR_get_error();
    return evp_rc;
  }
  evp_rc = EVP_Digest(sha1, HA_SHA1_BYTE_LEN, sha2, NULL, EVP_sha1(), NULL);
  if (!evp_rc) {
    evp_rc = ERR_get_error();
    return evp_rc;
  }
  boost::algorithm::hex(sha2, sha2 + HA_SHA1_BYTE_LEN, std::back_inserter(out));
  return HA_ERR_OK;
}

// extract cipher string for password, refer to 'sdbpasswd' source code
void ha_extract_mix_cipher_string(const string &mix_cipher,
                                  string &passwd_cipher, string &rand_array) {
  string unhex_mix_cipher;
  try {
    boost::algorithm::unhex(mix_cipher, std::back_inserter(unhex_mix_cipher));
    int low_pos = int(unhex_mix_cipher[0]) + 1;
    int arr1_len = unhex_mix_cipher[low_pos] + 2;
    passwd_cipher += unhex_mix_cipher.substr(1, low_pos - 1);
    rand_array += unhex_mix_cipher.substr(low_pos + 1, arr1_len - 2);

    int mid_pos = unhex_mix_cipher[low_pos + arr1_len - 1] + 1;
    int arr2_len = unhex_mix_cipher[mid_pos] + 2;

    passwd_cipher += unhex_mix_cipher.substr(low_pos + arr1_len,
                                             mid_pos - low_pos - arr1_len);
    rand_array += unhex_mix_cipher.substr(mid_pos + 1, arr2_len - 2);

    int high_pos = unhex_mix_cipher[mid_pos + arr2_len - 1] + 1;
    int arr3_len = unhex_mix_cipher[high_pos] + 1;
    passwd_cipher += unhex_mix_cipher.substr(mid_pos + arr2_len,
                                             high_pos - mid_pos - arr2_len);
    rand_array += unhex_mix_cipher.substr(high_pos + 1, arr3_len - 1);
    passwd_cipher += unhex_mix_cipher.substr(high_pos + arr3_len);
  } catch (std::exception &e) {
    std::string error_str = "parse cipher string error: ";
    throw std::runtime_error(error_str + e.what());
  }
}

int ha_evp_des_decrypt(const uchar *cipher, int len, uchar *out, uchar *key) {
  int evp_rc = 0, out_len = 0;
  assert(cipher != NULL);
  assert(out != NULL);

  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);
  EVP_DecryptInit_ex(&ctx, EVP_des_ecb(), NULL, key, NULL);
  evp_rc = EVP_DecryptUpdate(&ctx, out, &out_len, cipher, len);
  EVP_DecryptFinal_ex(&ctx, out, &out_len);
  EVP_CIPHER_CTX_cleanup(&ctx);

  evp_rc = evp_rc ? 0 : ERR_get_error();
  return evp_rc;
}

int ha_generate_des_key(uchar key[], const std::string &str) {
  uchar sha256[HA_SHA256_BYTE_LEN];
  int rc = ha_evp_digest(str, sha256, HA_EVP_SHA256);
  if (rc) {
    return rc;
  }
  memcpy(key, sha256, HA_DES_KEY_BYTE_LEN);
  return HA_ERR_OK;
}

// extract encrypted password and decrypt it
int ha_decrypt_password(const string &mix_hex_cipher, const string &token,
                        string &password) {
  string passwd_cipher, rand_array;
  int rc = 0;
  try {
    // divide mix cipher string into 'rand_array' and 'passwd_cipher'
    ha_extract_mix_cipher_string(mix_hex_cipher, passwd_cipher, rand_array);

    // generate des key
    uchar key[HA_DES_KEY_BYTE_LEN] = {0};
    rc = ha_generate_des_key(key, token + rand_array);
    if (rc) {
      return rc;
    }

    // allocate memory for password and decrypt password
    password = passwd_cipher;
    const uchar *cipher = (const uchar *)passwd_cipher.c_str();
    uchar *out = (uchar *)password.c_str();
    rc = ha_evp_des_decrypt(cipher, passwd_cipher.length(), out, key);
    if (rc) {
      return rc;
    }
    int len = password.length() - 1;
    while (0 == password[len]) {
      len--;
    }
    password = password.substr(0, len + 1);
  } catch (std::exception &e) {
    cout << "Error: decrypt password error: " << e.what() << endl;
    return HA_ERR_EXCEPTION;
  }
  return HA_ERR_OK;
}

// parse user's password from 'cipher file'
int ha_parse_password(const string &user, const string &token,
                      const string &file_name, string &password) {
  int rc = 0;
  ifstream crypt_ifs;
  string line, file_user, mix_cipher;
  size_t split_pos;
  bool found = false;

  password = "";
  crypt_ifs.open(file_name, ios::in);
  if (crypt_ifs.is_open()) {
    while (getline(crypt_ifs, line)) {
      split_pos = line.find(":");
      if (string::npos == split_pos) {
        continue;
      }
      file_user = line.substr(0, split_pos);
      if (user != file_user) {
        continue;
      } else if (split_pos + 1 >= line.length()) {
        found = true;
        break;
      }
      mix_cipher = line.substr(split_pos + 1);
      if (!file_user.empty() && user == file_user &&
          string::npos != split_pos) {
        rc = ha_decrypt_password(mix_cipher, token, password);
        HA_TOOL_RC_CHECK(rc, rc, "Error: %s", ha_error_string(rc).c_str());
        found = true;
        break;
      }
    }
    HA_TOOL_RC_CHECK(!found, HA_ERR_INVALID_USER,
                     "Error: can't find user '%s' in '%s'", user.c_str(),
                     file_name.c_str());
  } else {
    cerr << "Error: open file " << file_name << " error " << strerror(errno)
         << endl;
    return HA_ERR_SYS;
  }
  return rc;
}

// init default value for command line arguments
void ha_init_default_args(st_args &cmd_args) {
  cmd_args.is_user_set = cmd_args.is_password_set = false;
  cmd_args.key = "";
  cmd_args.force = false;
  cmd_args.host = HA_DEFAULT_HOST;
  cmd_args.token = "";
  cmd_args.file_name = HA_DEFAULT_CIPHER_FILE;
  cmd_args.verbose = false;

  cmd_args.is_inst_id_set = false;
  cmd_args.inst_host = "";
  cmd_args.inst_hostname = "";
  cmd_args.inst_port = 0;
}

// translate ssl error code to human-readable string
const std::string ha_error_string(int error_code) {
  std::string err_str;
  if (HA_ERR_RAND_STR_TOO_LONG == error_code) {
    err_str = "request random string is too long";
  } else {
    err_str = ERR_error_string(error_code, NULL);
  }
  return err_str;
}

// get error string for sequoiadb error code
const char *ha_sdb_error_string(sdbclient::sdb &conn, int rc) {
  static char err_buf[HA_BUF_LEN] = {0};
  memset(err_buf, '0', HA_BUF_LEN);
  bson::BSONObj result;

  if (conn.getLastErrorObj(result)) {
    sprintf(err_buf, "failed to get sequoiadb error, error code is: %d", rc);
  } else {
    const char *err = result.getStringField(SDB_FIELD_DESCRIPTION);
    if (0 == strlen(err)) {
      err = result.getStringField(SDB_FIELD_DETAIL);
    }
    snprintf(err_buf, HA_BUF_LEN, "%s", err);
  }
  return err_buf;
}

// host format: "hostname:port", decompose host
int ha_parse_host(const std::string &host, std::string &hostname, uint &port) {
  int rc = 0;

  size_t split_pos = host.find(":");
  HA_TOOL_RC_CHECK(string::npos == split_pos, HA_ERR_INVALID_PARAMETER,
                   "Error: invalid value '%s' for 'host'", host.c_str());

  hostname = host.substr(0, split_pos);
  std::string port_str = host.substr(split_pos + 1);
  try {
    port = std::stoi(port_str);
  } catch (std::exception &e) {
    cerr << "Error: invalid value for 'host', std::stoi failed" << endl;
    rc = HA_ERR_INVALID_PARAMETER;
  }
  return rc;
}

int ha_init_sequoiadb_connection(sdbclient::sdb &conn, ha_tool_args &cmd_args) {
  bool no_passwd_login = false;
  const char *sdb_err = "";
  int rc = 0;
  cmd_args.inst_group_name = HA_INST_GROUP_PREFIX + cmd_args.inst_group_name;
  if (cmd_args.is_password_set) {
    // if 'password' parameter is set, parameter 'user' must be set as well
    HA_TOOL_RC_CHECK(!cmd_args.is_user_set, HA_ERR_INVALID_PARAMETER,
                     "Error: lack of argument 'user'");

    // if user does not enter 'password' in visible way,
    // prompt user to enter password without echoing to the terminal
    if (cmd_args.password.empty()) {
      cmd_args.password = ha_get_password("Enter password: ", false);
    }
  } else if (cmd_args.is_user_set) {
    // if 'password' parameter is not set, and parameter 'user' is set
    // read user's password from cipher file created by sdbpassword
    rc = ha_parse_password(cmd_args.user, cmd_args.token, cmd_args.file_name,
                           cmd_args.password);
    HA_TOOL_RC_CHECK(rc, rc,
                     "Error: failed to decrypt password from '%s', "
                     "please check if 'token' or 'file' is correct.",
                     cmd_args.file_name.c_str());
  } else {
    no_passwd_login = true;
  }

  rc = ha_parse_host(cmd_args.host, cmd_args.hostname, cmd_args.port);
  HA_TOOL_RC_CHECK(rc, rc, "Error: 'host' is not in the correct format");

  if (no_passwd_login) {
    rc = conn.connect(cmd_args.hostname.c_str(), cmd_args.port);
  } else {
    rc = conn.connect(cmd_args.hostname.c_str(), cmd_args.port,
                      cmd_args.user.c_str(), cmd_args.password.c_str());
  }
  sdb_err = rc ? ha_sdb_error_string(conn, rc) : "";
  HA_TOOL_RC_CHECK(rc, rc,
                   "Error: failed to connect sequoiadb, sequoiadb error: %s",
                   sdb_err);
  return rc;
}
