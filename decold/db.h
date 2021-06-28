#ifndef DB_
#define DB_

#include "rocksdb/c.h"
#include <unistd.h>  // sysconf() - get CPU count
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <dirent.h>
#include "recipe.h"

extern rocksdb_writeoptions_t *writeoptions;
extern rocksdb_readoptions_t *readoptions;
extern rocksdb_t *db;

int db_init(const char *db_path, int overlay);
int db_write(const char *key, const char * value);
char * db_read(const char *key);
int db_is_key_exist(const char *key);
int db_delete(const char *key);
void db_close();
int64_t construct_db(int64_t demo_fid, const char *path);
char *db_read_by_int(int64_t key);

#endif