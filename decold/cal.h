#ifndef CAL_H_
#define CAL_H_

#include "recipe.h"

// struct summary_double
// {
//     fingerprint fp;
//     containerid cid;
//     int64_t size;
//     int64_t fid_s1; //fp->s1
//     int64_t order1;
//     int64_t fid_s2; //fp->s2
//     int64_t order2;
// };

struct summary_common
{
	fingerprint fp;
	containerid cid;
	int64_t size;
	int64_t fid;
	int64_t order;
};

struct file_fp
{
	int64_t fid;
	int64_t num;
	int64_t flag; //1-> dedup, 0-> stay
	fingerprint *fps; //sorted by order
	int64_t *sizes; //for size
	//int64_t *cids; //for container ids
};

struct migrate
{
	int64_t fid;
	int64_t total_num;
	fingerprint *fps; //sorted by order
	int64_t *arr;//0~total, for size; total~2*total, for if it exist
	// int64_t *sizes; //for size
	// int64_t *in; //if it exist
	//int64_t *cids;
};

struct top1
{
	//int64_t fid;
	//here can be a spark, lightpoint
	int64_t index_s;
	int64_t index_mr;
	int64_t enable; // enable the file to new S1
	fingerprint fp; //features
};

// struct top
// {
//     int64_t fid;
//     int64_t flag; //1-> the file meets the topk
//     int64_t size;
//     fingerprint *fps; //features
// };

// fp -> fid ,times
struct target
{
	fingerprint fp;//fp->s1
	int64_t refs;
	int64_t cid; //for container ids when create intersection recipe
};

void cal_inter(struct summary *s1, int64_t s1_count, struct summary *s2, int64_t s2_count, struct summary_common **scommon1, int64_t *sc1_count, struct summary_common **scommon2, int64_t *sc2_count);

void file_find(struct map_recipe *mr, int64_t mr_count, struct summary_common *scommon, int64_t sc_count, struct file_fp **fff, int64_t *ff_count, struct migrate **mm, int64_t *m_count, int64_t mig_count[5]);

void migration_find(struct migrate *m, int64_t m_count, struct summary *s_c, int64_t s_count);

void all_refs_find(struct summary *s, int64_t s_count, struct target **t, int64_t *t_count);

void target_refs_find(struct target *r, int64_t r_count, struct target **t, int64_t *t_count, struct file_fp *ff, int64_t ff_count);

void inter_t1_t2(fingerprint **t1_2, int64_t *t1_2_count, struct target *t1, int64_t t1_count, struct target *t2, int64_t t2_count);

int64_t check_in_r(struct target *r, int64_t count, fingerprint fp);

int64_t check_in_f(fingerprint *f, int64_t count, fingerprint fp);

void top1(struct summary **s1, int64_t *s1_count, struct summary **s2, int64_t *s2_count, \
struct map_recipe **mr1, int64_t *mr1_count, struct map_recipe **mr2, int64_t *mr2_count);

int64_t total_groups(struct summary *s, int64_t s_count, int64_t max_cid);

#endif