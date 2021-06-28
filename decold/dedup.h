#ifndef DEDUP_H_
#define DEDUP_H_

#include "cal.h"
#include "db.h"

//Note that intersection can do right now, but migration will be later

//delete old files and old recipes.
//create new files for new recipe and new meta
//chunkpointer
// struct new_recipe_meta
// {
// 	int64_t fid;
// 	int64_t chunknum; //chunknum;
// 	int64_t filesize;
// };

struct new_recipe
{
	fingerprint fp;
	int64_t size; //for size
	int64_t mark; //0->others cid, 1->new cid(but now is the old cid, since you should retrieve it)
	int64_t new_cid; //mark new cids;
};

//create new container for them;
// meta is the same, and =1 part is the same,
struct new_migration
{
	fingerprint fp;
	int64_t file_no;
	int64_t chunk_no;
	int64_t size; //for size
	int64_t old_cid; //mark old cid
	unsigned char *data;// if new_cid =0, *data is enable;
};

void stat_max_dedup(fingerprint **d1, int64_t *d1_count, fingerprint **d2, int64_t *d2_count, struct file_fp *ff1, \
	int64_t ff1_count, struct target *t1, int64_t t1_count, struct file_fp *ff2, int64_t ff2_count, struct target *t2, int64_t t2_count);

void stat_max_dedup_ignore(fingerprint **d1, int64_t *d1_count, fingerprint **d2, int64_t *d2_count, struct file_fp *ff1, \
	int64_t ff1_count, struct target *t1, int64_t t1_count, struct file_fp *ff2, int64_t ff2_count, struct target *t2, int64_t t2_count, int64_t r1_count, int64_t r2_count);

/*
void optimal_dedup(struct file_fp *ff1, int64_t ff1_count, struct target *t1, int64_t t1_count, \
struct file_fp *ff2, int64_t ff2_count, struct target *t2, int64_t t2_count, int64_t optimal_stat[5]);
*/

double create_recipe(const char *path, fingerprint *dedup, int64_t d_count, struct file_fp *ff, int64_t ff_count, \
struct target *r, int64_t r_count, struct map_recipe *mr, int64_t mr_count, int64_t stat[2]);

void create_migartion(const char *path1, struct migrate *m1, int64_t m1_count, struct target *r1, int64_t r1_count, struct target *r2, int64_t r2_count, \
	fingerprint *dedup1, int64_t d1_count, fingerprint *dedup2, int64_t d2_count, struct map_recipe *mr1, int64_t mr1_count, int64_t stat[]);

//consider how to deal with new.meta and new.recipe

//consider how to deal with migrate.meta and migrate.recipe
//Note that when cid =-1, it should get

#endif