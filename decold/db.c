#include "db.h"

rocksdb_writeoptions_t *writeoptions = NULL;
rocksdb_readoptions_t *readoptions = NULL;
rocksdb_t *db = NULL;

int remove_dir(const char *dir)
{
	char cur_dir[] = ".";
	char up_dir[] = "..";
	char dir_name[128];
	DIR *dirp;
	struct dirent *dp;
	struct stat dir_stat;

	if (0 != access(dir, F_OK)) {
		return 0;
	}

	if (0 > stat(dir, &dir_stat)) {
		perror("get directory stat error");
		return -1;
	}

	if (S_ISREG(dir_stat.st_mode)) {
		remove(dir);
	}
	else if (S_ISDIR(dir_stat.st_mode)) {
		dirp = opendir(dir);
		while ((dp = readdir(dirp)) != NULL) {
			if ((0 == strcmp(cur_dir, dp->d_name)) || (0 == strcmp(up_dir, dp->d_name))) {
				continue;
			}

			sprintf(dir_name, "%s/%s", dir, dp->d_name);
			remove_dir(dir_name);
		}
		closedir(dirp);

		rmdir(dir);
	}
	else {
		perror("unknow file type!");
	}

	return 0;
}

int db_init(const char *db_path, int overlay) {
	rocksdb_options_t *options = rocksdb_options_create();
	long cpus = sysconf(_SC_NPROCESSORS_ONLN);  // get # of online cores
	rocksdb_options_increase_parallelism(options, (int)(cpus));
	//rocksdb_options_optimize_level_style_compaction(options, 0);
	rocksdb_options_set_create_if_missing(options, 1);
	rocksdb_options_set_max_open_files(options, 32768);

	if (overlay) {
		if (-1 == remove_dir(db_path)) {
			printf("remove %s fail\n", db_path);
		}
		if (0 != mkdir(db_path, 666)) {
			printf("mkdir %s fail\n", db_path);
		}
	}

	char *err = NULL;
	db = rocksdb_open(options, db_path, &err);
	if (err) {
		printf("open db %s fail:%s\n", db_path, err);
		return -1;
	}

	writeoptions = rocksdb_writeoptions_create();
	readoptions = rocksdb_readoptions_create();

	return 0;
}

int db_write(const char *key, const char * value) {
	if (NULL == db || NULL == key || NULL == value)
		return -1;

	char *err = NULL;
	rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value) + 1, &err);
	if (err) {
		printf("write (%s, %s) failed:%s\n", key, value, err);
		return -1;
	}

	return 0;
}

char *db_read(const char *key) {
	if (NULL == db || NULL == key)
		return NULL;

	char *err = NULL;
	size_t len;
	char *returned_value = rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
	if (err)
	{
		printf("db read key:%s fail\n", key);
		return NULL;
	}
	if (NULL == returned_value) {
		printf("db key:%s not exist\n", key);
	}
	return returned_value;
}

char *db_read_by_int(int64_t key) {
	char key_str[128] = { 0 };
	sprintf(key_str, "%ld", key);
	return db_read(key_str);
}

int db_is_key_exist(const char *key) {
	if (NULL == db || NULL == key)
		return -1;

	char *err = NULL;
	size_t len;
	char *returned_value = rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
	if (err)
		return -1;
	else if (!returned_value) {
		return 0;
	}
	else {
		free(returned_value);
		return 1;
	}
}

int db_delete(const char *key) {
	if (NULL == db || NULL == key)
		return -1;

	char *err = NULL;
	rocksdb_delete(db, writeoptions, key, strlen(key), &err);
	if (err) {
		printf("delete %s err:%s\n", key, err);
		return -1;
	}

	return 0;
}

void db_close() {
	rocksdb_close(db);
}

static struct fileRecipeMeta *new_file_recipe_meta(char *name)
{
	struct fileRecipeMeta *r = (struct fileRecipeMeta *) malloc(sizeof(struct fileRecipeMeta));
	r->filename = (char *)malloc((strlen(name) + 1) * sizeof(char));
	strcpy(r->filename, name);
	r->chunknum = 0;
	r->filesize = 0;
	r->fid = 0;
	return r;
}

int64_t construct_db(int64_t demo_fid, const char *path)
{
	char path_meta[100];
	sprintf(path_meta, "%s%s", path, "bv0.meta");

	//printf("%s;%s\n",path_meta,path_recipe);
	FILE *meta_fp = fopen(path_meta, "r");

	if (!meta_fp)
		perror("\nFILE OPEN FAULT!\n");

	//head of metadata file, some for backup job
	int32_t bv_num;
	int deleted;
	int64_t number_of_files;
	int64_t number_of_chunks;
	int pathlen;

	//backup info
	fseek(meta_fp, 0, SEEK_SET);
	fread(&bv_num, sizeof(bv_num), 1, meta_fp);
	fread(&deleted, sizeof(deleted), 1, meta_fp);
	fread(&number_of_files, sizeof(number_of_files), 1, meta_fp);
	fread(&number_of_chunks, sizeof(number_of_chunks), 1, meta_fp);

	//path
	fread(&pathlen, sizeof(int), 1, meta_fp);
	char path_t[pathlen + 1];
	fread(path_t, pathlen, 1, meta_fp);
	path_t[pathlen] = 0;

	printf("files:%ld, chunks:%ld\n", number_of_files, number_of_chunks);
	int64_t i;
	for (i = 0; i < number_of_files; i++)
	{
		//meta start
		int len;
		fread(&len, sizeof(len), 1, meta_fp);
		char name[len + 1];

		fread(name, len, 1, meta_fp);
		name[len] = 0;

		struct fileRecipeMeta *r = new_file_recipe_meta(name);

		fread(&r->chunknum, sizeof(r->chunknum), 1, meta_fp);
		fread(&r->filesize, sizeof(r->filesize), 1, meta_fp);

		char fid_str[128] = { 0 };
		sprintf(fid_str, "%ld", demo_fid++);
		if (0 != db_write(fid_str, name)) {
			printf("db write (%s,%s) fail\n", name, fid_str);
			return -1;
		}

		/*
		if (0 == i%1000000) {
		printf("db process:%ld\n", demo_fid);
		}
		*/
	}

	fclose(meta_fp);

	return demo_fid;
}