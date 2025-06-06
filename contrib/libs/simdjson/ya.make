# Generated by devtools/yamaker from nixpkgs 24.05.

LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(3.13.0)

ORIGINAL_SOURCE(https://github.com/simdjson/simdjson/archive/v3.13.0.tar.gz)

ADDINCL(
    GLOBAL contrib/libs/simdjson/include
    contrib/libs/simdjson/src
)

NO_COMPILER_WARNINGS()

NO_UTIL()

CFLAGS(
    -DSIMDJSON_AVX512_ALLOWED=1
    -DSIMDJSON_UTF8VALIDATION=1
)

SRCS(
    src/simdjson.cpp
)

END()
