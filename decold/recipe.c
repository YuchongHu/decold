#include "recipe.h"

// int64_t demo_fid =0;

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

static int64_t read_next_n_chunk_pointers(FILE *recipe_fp, int64_t fid, struct summary *s1, int64_t *s1_count, int64_t num)
{
	struct chunkPointer *cp = (struct chunkPointer *) malloc(sizeof(struct chunkPointer));

	int64_t max_cid = 0;

	int64_t i;
	for (i = 0; i < num; i++)
	{
		fread(&(cp->fp), sizeof(fingerprint), 1, recipe_fp);
		fread(&(cp->id), sizeof(containerid), 1, recipe_fp);
		fread(&(cp->size), sizeof(int32_t), 1, recipe_fp);

		// Ignore segment boundaries
		//only count really chunk, escape FILE and SEGMENT
		if (cp->id == 0 - CHUNK_SEGMENT_START || cp->id == 0 - CHUNK_SEGMENT_END)
		{
			i--;
			continue;
		};

		memcpy(s1[*s1_count].fp, cp->fp, sizeof(fingerprint));
		s1[*s1_count].fid = fid;
		s1[*s1_count].order = i;
		s1[*s1_count].size = cp->size;
		s1[*s1_count].cid = cp->id;

		if (cp->id > max_cid)
			max_cid = cp->id;

		if (LEVEL >= 2)
		{
			char code[41];
			hash2code(s1[*s1_count].fp, code);
			code[40] = 0;
			VERBOSE("   CHUNK:fid=[%8" PRId64 "], order=%" PRId64 ", size=%" PRId64 ", container_id=%" PRId64 ", fp=%s\n", \
				s1[*s1_count].fid, s1[*s1_count].order, s1[*s1_count].size, s1[*s1_count].cid, code);
		}

		(*s1_count)++;
	}
	free(cp);

	return max_cid;
}

int64_t read_recipe(int64_t demo_fid, const char *path, struct summary **s1_t, int64_t *s1_count, struct map_recipe **mr_t, int64_t *mr_count, int64_t *empty_count, int64_t *max_cid)
{
	char path_meta[100], path_recipe[100];
	sprintf(path_meta, "%s%s", path, "bv0.meta");
	sprintf(path_recipe, "%s%s", path, "bv0.recipe");

	//printf("%s;%s\n",path_meta,path_recipe);
	FILE *meta_fp = fopen(path_meta, "r");
	FILE *recipe_fp = fopen(path_recipe, "r");

	if (!(meta_fp && recipe_fp))
		perror("\nFILE OPEN FAULT!\n");

	//head of metadata file, some for backup job
	int32_t bv_num;
	int deleted;
	int64_t number_of_files;
	int64_t number_of_chunks;

	//backup info
	fseek(meta_fp, 0, SEEK_SET);
	fread(&bv_num, sizeof(bv_num), 1, meta_fp);
	fread(&deleted, sizeof(deleted), 1, meta_fp);
	fread(&number_of_files, sizeof(number_of_files), 1, meta_fp);
	fread(&number_of_chunks, sizeof(number_of_chunks), 1, meta_fp);

	int pathlen;
	//path
	fread(&pathlen, sizeof(int), 1, meta_fp);
	char path_t[pathlen + 1];

	fread(path_t, pathlen, 1, meta_fp);
	path_t[pathlen] = 0;

	//fp->fid
	//int64_t s1_count=0, mr_count=0;
	struct summary *s1 = (struct summary *)malloc(number_of_chunks * sizeof(struct summary));
	struct map_recipe *mr = (struct map_recipe *)malloc(number_of_files * sizeof(struct map_recipe));

	int64_t i;
	for (i = 0; i < number_of_files; i++)
	{
		//meta start
		mr[*mr_count].offset_m = ftell(meta_fp);
		int len;
		fread(&len, sizeof(len), 1, meta_fp);
		char name[len + 1];

		fread(name, len, 1, meta_fp);
		name[len] = 0;

		struct fileRecipeMeta *r = new_file_recipe_meta(name);

		fread(&r->chunknum, sizeof(r->chunknum), 1, meta_fp);
		fread(&r->filesize, sizeof(r->filesize), 1, meta_fp);

		//TO-DO fid by myself, it can be lookup by RocksDB
		r->fid = demo_fid++;

		//map_recipe
		mr[*mr_count].fid = r->fid;
		mr[*mr_count].size = r->filesize;
		mr[*mr_count].chunknum = r->chunknum;
		//recipe start
		mr[*mr_count].offset_r = ftell(recipe_fp);

		if (LEVEL >= 2)
		{
			VERBOSE("\nMAP RECIEP {%s}: mr_count= %" PRId64 ", num=%" PRId64 ",size=%" PRId64 ",fid=%" PRId64 ";offset_m=%" PRId64 ";offset_r=%" PRId64 ";[%s]\n", path, *mr_count, r->chunknum, \
				r->filesize, r->fid, mr[*mr_count].offset_m, mr[*mr_count].offset_r, name);
			/*
			VERBOSE("MAP RECIEP {%s}: mr_count= %" PRId64 ", num=%" PRId64 ",size=%" PRId64 ",fid=%" PRId64 ";[%s]\n", path, *mr_count, r->chunknum, \
			r->filesize, r->fid, name);
			*/
		}

		if (r->filesize == 0)
			(*empty_count)++;

		(*mr_count)++;
		//summary
		int max = read_next_n_chunk_pointers(recipe_fp, r->fid, s1, s1_count, r->chunknum);
		if (max > *max_cid)
			*max_cid = max;
	}

	//assign
	*s1_t = s1;
	*mr_t = mr;

	if (LEVEL >= 1)
		VERBOSE("\nFINISH META AND RECIPE FILES, MAX_CID: %"PRId64"!\n", *max_cid);

	fclose(meta_fp);
	fclose(recipe_fp);

	return demo_fid;
}

static void get_chunk_from_container(unsigned char *vv, struct container *c, fingerprint *fp)
{
	struct metaEntry* me = g_hash_table_lookup(c->meta.map, fp);
	if (!c->meta.map)
		VERBOSE("ASSERT, META.MAP == NULL!\n");

	if (!me)
		VERBOSE("ASSERT, ME == NULL!\n");

	if (me)
		memcpy(vv, c->data + me->off, me->len);
	//print_unsigned(vv,me->len);
}

//for Destor func, locality
void retrieve_from_container(FILE* pool_fp, containerid cid, unsigned char** v, fingerprint *fps, int64_t num)
{
	struct container *c = (struct container*) malloc(sizeof(struct container));
	c->meta.chunk_num = 0;
	c->meta.data_size = 0;
	c->meta.id = cid;
	c->meta.map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL, free);

	unsigned char *cur = 0;

	//only malloc the meta data
	c->data = malloc(CONTAINER_SIZE);

	fseek(pool_fp, cid * CONTAINER_SIZE + 8, SEEK_SET);
	fread(c->data, CONTAINER_SIZE, 1, pool_fp);
	cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];

	int i;
	unser_declare;
	unser_begin(cur, CONTAINER_META_SIZE);

	unser_int64(c->meta.id);
	unser_int32(c->meta.chunk_num);
	unser_int32(c->meta.data_size);

	for (i = 0; i < c->meta.chunk_num; i++)
	{
		struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		g_hash_table_insert(c->meta.map, &me->fp, me);
	}

	if (!c->meta.map)
		VERBOSE("ASSERT, META.MAP == NULL!\n");

	unser_end(cur, CONTAINER_META_SIZE);

	for (i = 0; i < num; i++)
	{
		get_chunk_from_container(v[i], c, &(fps[i]));
	}

	free(c->data);
	free(c);
}