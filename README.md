# SequoiaSQL-MySQL Storage Engine

SequoiaSQL-MySQL Storage Engine is a distributed MySQL storage engine.

It currently supports SequoiaDB 3.x as the backend database, and it will be extended to multiple databases such like MongoDB/Redis and etc...

In order to take advantages of scalability and performance, SequoiaSQL-MySQL Storage Engine can be used to replace InnoDB and store user data/index/lob in the backend distributed database.


## Building
1. Get the source code of SequoiaSQL-MySQL and SequoiaDB C++ driver 3.4.
```bash
git clone http://gitlab.sequoiadb.com/sequoiadb/sequoiasql-mysql.git sequoiasql-mysql
```
2. Build the SequoiaSQL-MySQL.
```bash
python3 build.py --sdbdriver </path/to/sequoiadb/cpp/driver> -t mysql-5.7.25 -i </path/to/install/mysql/> --clean --archivetest --dd
```
SequoiaSQL-MySQL Storage Engine also supports the MySQL(5.7.24/5.7.28) and MariaDB(10.4.6). Build different version of MySQL or MariaDB with argument `-t projectType`. For example:
```bash
python3 build.py --sdbdriver </path/to/sequoiadb/cpp/driver> -t mariadb-10.4.6 -i </path/to/install/mysql/> --clean --archivetest --dd
```

## Coding Guidelines

According to [MySQL coding guidelines](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_CODING_GUIDELINES.html), we use the [Google C++ coding style](https://google.github.io/styleguide/cppguide.html).

We use [`clang-format`](http://clang.llvm.org/docs/ClangFormat.html) to format source code by 'Google' style with some exceptions. The '.clang-format' file contains all the options we used.

The `.clang-format` file is dumped from the tool by following command:
```bash
clang-format -style=google -dump-config > .clang-format
```

And we changed following options:
```
SortIncludes: false
AllowShortFunctionsOnASingleLine: Inline
AllowShortIfStatementsOnASingleLine: false
AllowShortLoopsOnASingleLine: false
```

You can directly use clang-format command line or [the plugin in VSCode](https://marketplace.visualstudio.com/items?itemName=xaver.clang-format) if you use VSCode. Remember to use the `.clang-format` file as style.

## License

License under the GPL License, Version 2.0. See LICENSE for more information.
Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.
