#ifndef __RECIPE_H
#define __RECIPE_H

#include "common.h"

//Destor recipe meta struct
struct fileRecipeMeta
{
	int64_t chunknum;
	int64_t filesize;

	//filename may do not need, with NameNode, we only need fid.
	char *filename;
	int64_t fid;
};
//Destor recipe struct
struct chunkPointer
{
	fingerprint fp;
	containerid id;
	int32_t size;
};

//we ignore the container, but recorder the size and order
struct summary
{
	fingerprint fp;
	int64_t order; // record the sequence of fp in the file
	int64_t size; //recorder the size
	int64_t fid;
	containerid cid;
};

struct map_recipe
{
	int64_t fid;

	int64_t size;
	int64_t chunknum; //for to choose intersection & migration
	// TO-DO: in which recipe
	int64_t offset_m;
	int64_t offset_r;
};

#define CONTAINER_SIZE (4194304ll) //4MB
#define CONTAINER_META_SIZE (32768ll) //32KB
#define CONTAINER_HEAD 16
#define CONTAINER_META_ENTRY 28
#define TEMPORARY_ID (-1L)

//Destor data
struct containerMeta
{
	containerid id;

	int32_t data_size;
	int32_t chunk_num;
	GHashTable *map;
};

struct container
{
	struct containerMeta meta;
	unsigned char *data;
};

struct metaEntry
{
	int32_t off;
	int32_t len;
	fingerprint fp;
};

int64_t read_recipe(int64_t demo_fid, const char *path, struct summary **s1, int64_t *s1_count, struct map_recipe **mr, int64_t *mr_count, int64_t *empty_count, int64_t *max_cid);

//Destor func
void retrieve_from_container(FILE *pool_fp, containerid cid, unsigned char** v, fingerprint *fps, int64_t num);

//void get_chunk_in_container();

#endif