#include "dedup.h"

static int comp_fp(const void *fp1, const void *fp2)
{
	return comp_code((unsigned char*)fp1, (unsigned char*)fp2);
}

//for dedup ff1, and then dedup ff2
//TO-DO flag can be considered carefully
void stat_max_dedup(fingerprint **d1, int64_t *d1_count, fingerprint **d2, int64_t *d2_count, struct file_fp *ff1, \
	int64_t ff1_count, struct target *t1_origin, int64_t t1_count, struct file_fp *ff2, int64_t ff2_count, struct target *t2_origin, int64_t t2_count)
{
	int64_t i, j;
	//copy t1,t2
	int64_t *t1 = (int64_t *)malloc(t1_count * sizeof(int64_t));
	int64_t *t2 = (int64_t *)malloc(t2_count * sizeof(int64_t));

	int64_t inter1 = 0;
	for (i = 0; i < t1_count; i++)
	{
		t1[i] = t1_origin[i].refs;
	}
	for (i = 0; i < t2_count; i++)
	{
		t2[i] = t2_origin[i].refs;
	}

	for (i = 0; i < ff1_count; i++)
	{
		int64_t num = ff1[i].num;
		ff1[i].flag = 1; //dedup, it should be find in other group
		for (j = 0; j < num; j++)
		{
			int64_t mid = check_in_r(t1_origin, t1_count, (ff1[i].fps)[j]);
			if (LEVEL >= 2)
			{
				VERBOSE("No.%" PRId64 ",FID [%8" PRId64 "].fps[%" PRId64 "], find in the t1, mid={%" PRId64 "}, DEDUP IT IN THIS GROUP.\n", inter1++, ff1[i].fid, j, mid);
				char code[41];
				hash2code((ff1[i].fps)[j], code);
				code[40] = 0;

				char code1[41];
				hash2code(t1_origin[mid].fp, code1);
				code1[40] = 0;
				VERBOSE("   num=%" PRId32 ", code=%s; t1 mid=%s\n", num, code, code1);
			}
			t1[mid]--;
		}
	}

	//T1=0,sorted
	fingerprint *dedup1 = (fingerprint *)malloc(t1_count * sizeof(fingerprint));

	for (i = 0; i < t1_count; i++)
	{
		if (t1[i] == 0)
		{
			memcpy(dedup1[*d1_count], t1_origin[i].fp, sizeof(fingerprint));
			(*d1_count)++;
		}
	}

	//rid of T1=0
	int64_t inter2 = 0;
	for (i = 0; i < ff2_count; i++)
	{
		int64_t num = ff2[i].num;
		for (j = 0; j < num; j++)
		{
			//group2 can not dedup that have dedup in the group1
			int64_t mid = check_in_f(dedup1, *d1_count, (ff2[i].fps)[j]);

			//if in dedup1, can not del;
			if (mid != -1)
			{
				if (LEVEL >= 2)
					VERBOSE("No.%" PRId64 ",FID [%8" PRId64 "].fps[%" PRId64 "], find in the dedup1=0, mid={%" PRId64 "}, ALREADY DEDUP IN OTHER GROUP.\n", inter2++, ff2[i].fid, j, mid);
				break;
			}
			else
			{
				ff2[i].flag = 1; //dedup, it should be find in other group
				int64_t m = check_in_r(t2_origin, t2_count, (ff2[i].fps)[j]);
				if (LEVEL >= 2)
				{
					char code[41];
					hash2code((ff2[i].fps)[j], code);
					code[40] = 0;
					VERBOSE("No.%" PRId64 ",FID [%8" PRId32 "].fps[%" PRId32 "], can not find in dedup=0, code=%s, PLUS THIS ONE.\n", inter2++, ff2[i].fid, j, code);
				}

				t2[m]--;
			}
		}
	}

	//T2=0,sorted
	fingerprint *dedup2 = (fingerprint *)malloc(t2_count * sizeof(fingerprint));

	for (i = 0; i < t2_count; i++)
	{
		if (t2[i] == 0)
		{
			memcpy(dedup2[*d2_count], t2_origin[i].fp, sizeof(fingerprint));
			(*d2_count)++;
		}
	}

	dedup1 = (fingerprint *)realloc(dedup1, *d1_count * sizeof(fingerprint));
	dedup2 = (fingerprint *)realloc(dedup2, *d2_count * sizeof(fingerprint));

	*d1 = dedup1;
	*d2 = dedup2;

	free(t1);
	free(t2);
}

//ignore refs>1
void stat_max_dedup_ignore(fingerprint **d1, int64_t *d1_count, fingerprint **d2, int64_t *d2_count, struct file_fp *ff1, \
	int64_t ff1_count, struct target *t1_origin, int64_t t1_count, struct file_fp *ff2, int64_t ff2_count, struct target *t2_origin, int64_t t2_count, int64_t r1_count, int64_t r2_count)
{
	int64_t i, j;
	//copy t1,t2
	int64_t *t1 = (int64_t *)malloc(t1_count * sizeof(int64_t));
	int64_t *t2 = (int64_t *)malloc(t2_count * sizeof(int64_t));

	int64_t inter1 = 0;
	for (i = 0; i < t1_count; i++)
	{
		t1[i] = t1_origin[i].refs;
	}
	for (i = 0; i < t2_count; i++)
	{
		t2[i] = t2_origin[i].refs;
	}

	//T1=0,sorted
	fingerprint *dedup1 = (fingerprint *)malloc(r1_count * sizeof(fingerprint));

	for (i = 0; i < ff1_count; i++)
	{
		int64_t num = ff1[i].num;
		ff1[i].flag = 1; //dedup, it should be find in other group
		for (j = 0; j < num; j++)
		{
			int64_t mid = check_in_r(t1_origin, t1_count, (ff1[i].fps)[j]);
			if (mid != -1)
			{
				if (LEVEL >= 2)
				{
					VERBOSE("No.%" PRId64 ",FID [%8" PRId64 "].fps[%" PRId64 "], find in the t1, mid={%" PRId64 "}, DEDUP IT IN THIS GROUP.\n", inter1++, ff1[i].fid, j, mid);
					char code[41];
					hash2code((ff1[i].fps)[j], code);
					code[40] = 0;

					char code1[41];
					hash2code(t1_origin[mid].fp, code1);
					code1[40] = 0;
					VERBOSE("   num=%" PRId32 ", code=%s; t1 mid=%s\n", num, code, code1);
				}
				t1[mid]--;
			}
			else
			{
				memcpy(dedup1[*d1_count], (ff1[i].fps)[j], sizeof(fingerprint));
				(*d1_count)++;
			}
		}
	}

	for (i = 0; i < t1_count; i++)
	{
		if (t1[i] == 0)
		{
			memcpy(dedup1[*d1_count], t1_origin[i].fp, sizeof(fingerprint));
			(*d1_count)++;
		}
	}

	//sort dedup1;
	qsort(dedup1, *d1_count, sizeof(fingerprint), comp_fp);

	//T2=0
	fingerprint *dedup2 = (fingerprint *)malloc(r2_count * sizeof(fingerprint));

	//rid of T1=0
	int64_t inter2 = 0;
	for (i = 0; i < ff2_count; i++)
	{
		int64_t num = ff2[i].num;
		for (j = 0; j < num; j++)
		{
			//group2 can not dedup that have dedup in the group1
			int64_t mid = check_in_f(dedup1, *d1_count, (ff2[i].fps)[j]);

			//if in dedup1, can not del;
			if (mid != -1)
			{
				if (LEVEL >= 2)
					VERBOSE("No.%" PRId64 ",FID [%8" PRId64 "].fps[%" PRId64 "], find in the dedup1=0, mid={%" PRId64 "}, ALREADY DEDUP IN OTHER GROUP.\n", inter2++, ff2[i].fid, j, mid);
				break;
			}
			else
			{
				ff2[i].flag = 1; //dedup, it should be find in other group
				int64_t m = check_in_r(t2_origin, t2_count, (ff2[i].fps)[j]);
				if (m != -1)
				{
					if (LEVEL >= 2)
					{
						char code[41];
						hash2code((ff2[i].fps)[j], code);
						code[40] = 0;
						VERBOSE("No.%" PRId64 ",FID [%8" PRId32 "].fps[%" PRId32 "], can not find in dedup=0, code=%s, PLUS THIS ONE.\n", inter2++, ff2[i].fid, j, code);
					}

					t2[m]--;
				}
				else
				{
					memcpy(dedup2[*d2_count], (ff2[i].fps)[j], sizeof(fingerprint));
					(*d2_count)++;
					char code[41];
					hash2code((ff2[i].fps)[j], code);
					code[40] = 0;
					//VERBOSE("No.%" PRId64 ",FID [%8" PRId32 "].fps[%" PRId32 "], REFS =1 JUST PUT code=%s.\n", inter2++, ff2[i].fid, j, code);
				}
			}
		}
	}

	for (i = 0; i < t2_count; i++)
	{
		if (t2[i] == 0)
		{
			memcpy(dedup2[*d2_count], t2_origin[i].fp, sizeof(fingerprint));
			(*d2_count)++;
		}
	}

	//sort dedup1;
	qsort(dedup2, *d2_count, sizeof(fingerprint), comp_fp);

	dedup1 = (fingerprint *)realloc(dedup1, *d1_count * sizeof(fingerprint));
	dedup2 = (fingerprint *)realloc(dedup2, *d2_count * sizeof(fingerprint));

	*d1 = dedup1;
	*d2 = dedup2;

	free(t1);
	free(t2);
}

/*
void optimal_dedup(struct file_fp *ff1, int64_t ff1_count, struct target *t1,int64_t t1_count, \
struct file_fp *ff2, int64_t ff2_count, struct target *t2,int64_t t2_count, int64_t optimal_stat[5])
{
}
*/

//-1 -> not found
//mr
int64_t check_in_mr(struct map_recipe *mr, int64_t mr_count, int64_t fid)
{
	//binarySearch
	int64_t begin = 0, end = mr_count - 1;
	while (begin <= end)
	{
		int64_t mid = (begin + end) / 2;
		if (fid == mr[mid].fid)
		{
			//VERBOSE("SUCCESS\n");
			return mid;
		}
		else if (fid > mr[mid].fid)
			begin = mid + 1;
		else
			end = mid - 1;
	}
	return -1;
}

//real experiment TO-DO jobs.
// create new meta and recipe in other group
//t means t2
double create_recipe(const char *path, fingerprint *dedup, int64_t d_count, struct file_fp *ff, int64_t ff_count, \
struct target *r, int64_t r_count, struct map_recipe *mr, int64_t mr_count, int64_t stat[])
{
	double time_1 = 0;
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	char path_meta[100], path_recipe[100];
	sprintf(path_meta, "%s%s", path, "bv0.meta");
	sprintf(path_recipe, "%s%s", path, "bv0.recipe");

	//printf("%s;%s\n",path_meta,path_recipe);
	FILE *meta_fp = fopen(path_meta, "r");
	FILE *recipe_fp = fopen(path_recipe, "r");

	if (!(meta_fp && recipe_fp))
		perror("\nOLD FILE OPEN FAULT!\n");

	char new_path_meta[100], new_path_recipe[100];
	sprintf(new_path_meta, "%s%s", path, "new.meta");
	sprintf(new_path_recipe, "%s%s", path, "new.recipe");
	//a+, but test like w+s
	FILE *new_meta_fp = fopen(new_path_meta, "w+");
	FILE *new_recipe_fp = fopen(new_path_recipe, "w+");

	if (!(new_meta_fp && new_recipe_fp))
		perror("\nNEW FILE OPEN FAULT!\n");

	int64_t i, j;

	//if we use fwrite, it can use, but we use fprintf
	//struct new_recipe_meta *new_r_m=(struct new_recipe_meta*)malloc(sizeof(struct new_recipe_meta));
	int64_t file_count = 0, chunk_size = 0;
	int64_t *visit = (int64_t *)calloc(1, d_count * sizeof(int64_t));

	fprintf(new_meta_fp, "TOTAL DEDUP OF INTERSECTION FILE = %16"PRId64";\n", file_count);

	for (i = 0; i < ff_count; i++)
	{
		if (ff[i].flag == 1) //flag =1, intersection
		{
			file_count++;
			//create a recipe meta
			// new_r_m->fid=ff[i].fid;
			// new_r_m->chunknum=ff[i].chunknum;
			// new_r_m->filesize=ff[i].size;

			//write to new_meta_fp
			//find in mr
			//mr sorted by fid
			int64_t mid1 = check_in_mr(mr, mr_count, ff[i].fid);

			if (mid1 == -1)
				VERBOSE("ASSERT NEW RECIPE FAULT [%8"PRId64"], MAP RECIPE NOT FOUND!\n", ff[i].fid);

			// fwrite(&(ff[i].fid),sizeof(int64_t),1,new_meta_fp);
			// fwrite(&(mr[mid1].chunknum),sizeof(int64_t),1,new_meta_fp);
			// fwrite(&(mr[mid1].size),sizeof(int64_t),1,new_meta_fp);
			char *file_path = db_read_by_int(ff[i].fid);
			if (NULL == file_path) {
				printf("read file:%ld failed\n", ff[i].fid);
			}
			uint64_t path_len = strlen(file_path);
			//VERBOSE("%"PRId64";%"PRId32";%"PRId32";\n", ff[i].fid, mr[mid1].chunknum, mr[mid1].size);
			fprintf(new_meta_fp, "No. %16"PRId64"; fid= %16"PRId64"; chunknum= %16"PRId64"; size= %16"PRId64"; len=%16"PRId64"; name=%s;\n", i, ff[i].fid, mr[mid1].chunknum, mr[mid1].size, path_len, file_path);

			int64_t num = ff[i].num;
			for (j = 0; j < num; j++)
			{
				int64_t mid2 = check_in_f(dedup, d_count, (ff[i].fps)[j]);

				if (mid2 != -1)
				{
					if (visit[mid2] != 1)
					{
						chunk_size = chunk_size + (ff[i].sizes)[j];
						visit[mid2] = 1;
					}

					//we can use the buffer tech

					//append a receipe;
					struct new_recipe *new_r = (struct new_recipe*)malloc(sizeof(struct new_recipe));
					memcpy(new_r->fp, (ff[i].fps)[j], sizeof(fingerprint));
					new_r->size = (ff[i].sizes)[j];

					//cal the new cid from S2;
					int64_t mid3 = check_in_r(r, r_count, (ff[i].fps)[j]);
					if (mid3 == -1)
						VERBOSE("   ASSERT NEW RECIPE FAULT, OTHER T NOT FOUND!\n");

					new_r->new_cid = r[mid3].cid;

					//write a new recipe to file
					fwrite(new_r, sizeof(struct new_recipe), 1, new_recipe_fp);
					// int64_t k;
					// for(k=0;k<20;k++)
					//     fprintf(new_recipe_fp,"%c",((ff[i].fps)[j])[k]);
					// fprintf(new_recipe_fp, ",%"PRId64",%"PRId64"\n", r[mid3].cid,(ff[i].sizes)[j]);

					//del the old recipe in recipe fp, we can nonlyu use the re-write
					//TO-DO, how to realize????

					//find mr, mid1, we can locate the offset_m, offset_r
				}
			}

			//del the old recipe meta fp
			//TO-DO, how to realize????
		}
	}

	stat[0] = file_count;
	stat[1] = chunk_size;
	free(visit);

	fseek(new_meta_fp, 0L, SEEK_SET);
	fprintf(new_meta_fp, "TOTAL DEDUP OF INTERSECTION FILE = %16"PRId64";\n", file_count);

	fclose(meta_fp);
	fclose(recipe_fp);
	fclose(new_meta_fp);
	fclose(new_recipe_fp);

	TIMER_END(1, time_1);
	if (LEVEL >= 1)
	{
		VERBOSE("\nGROUP INTERSECTION RECIPE WRITE TIME:  %lf (s);\n", time_1);
	}

	return time_1;
}

// //create migration meta recipe
void create_migartion(const char *path1, struct migrate *m1, int64_t m1_count, struct target *r1, int64_t r1_count, struct target *r2, int64_t r2_count, \
	fingerprint *dedup1, int64_t d1_count, fingerprint *dedup2, int64_t d2_count, struct map_recipe *mr1, int64_t mr1_count, int64_t stat[])
{
	double time_1 = 0;
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	int64_t i, j;
	//S1
	char path_meta[100], path_recipe[100];
	sprintf(path_meta, "%s%s", path1, "bv0.meta");
	sprintf(path_recipe, "%s%s", path1, "bv0.recipe");

	//printf("%s;%s\n",path_meta,path_recipe);
	FILE *meta1_fp = fopen(path_meta, "r");
	FILE *recipe1_fp = fopen(path_recipe, "r");

	if (!(meta1_fp && recipe1_fp))
		perror("\nOLD MIGRATION FILE OPEN FAULT!\n");

	char m_path_meta[100], m_path_recipe[100];
	sprintf(m_path_meta, "%s%s", path1, "migrate_group.meta");
	sprintf(m_path_recipe, "%s%s", path1, "migrate_group.recipe");
	//a+, but test like w+
	FILE *m1_meta_fp = fopen(m_path_meta, "w+");
	FILE *m1_recipe_fp = fopen(m_path_recipe, "w+");

	if (!(m1_meta_fp && m1_recipe_fp))
		perror("\nNEW MIGRATION FILE OPEN FAULT!\n");

	struct target *r1_new, *r2_new;
	int64_t r1_count_new = 0, r2_count_new = 0;

	//rid of T=0
	r1_new = (struct target*)malloc(r1_count *sizeof(struct target));
	r2_new = (struct target*)malloc(r2_count *sizeof(struct target));

	for (i = 0; i < r1_count; i++)
	{
		//you do not del

		if (check_in_f(dedup1, d1_count, r1[i].fp) == -1)
			r1_new[r1_count_new++] = r1[i];
	}

	for (i = 0; i < r2_count; i++)
	{
		if (check_in_f(dedup2, d2_count, r2[i].fp) == -1)
			r2_new[r2_count_new++] = r2[i];
	}

	r1_new = (struct target*)realloc(r1_new, r1_count_new * sizeof(struct target));
	r2_new = (struct target*)realloc(r2_new, r2_count_new * sizeof(struct target));

	//whether m is in the intersection
	int64_t file_count1 = 0, m_chunk1 = 0, m_size1 = 0, d_chunk1 = 0, d_size1 = 0;

	int64_t total_mark1 = 0;

	fprintf(m1_meta_fp, "TOTAL MIGRATION FILE = %16"PRId64", MARK = %16"PRId64";\n", file_count1, total_mark1);

	int64_t *r1_refs = (int64_t*)malloc(r1_count_new*sizeof(int64_t));

	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < r1_count_new; i++)
		{
			char code[41];
			hash2code(r1_new[i].fp, code);
			code[40] = 0;
			VERBOSE("No.%" PRId64 ", MIGARTION X refs={%" PRId64 "}, fp= %s\n", i, r1_new[i].refs, code);
		}
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < r2_count_new; i++)
		{
			char code[41];
			hash2code(r2_new[i].fp, code);
			code[40] = 0;
			VERBOSE("No.%" PRId64 ", MIGARTION Y refs={%" PRId64 "}, fp= %s\n", i, r2_new[i].refs, code);
		}
		VERBOSE("\n----------------------------------------------------------------\n");
	}

	if (LEVEL >= 1)
		VERBOSE("\nMIGARTION X (%" PRId64 ") and Y (%" PRId64 ") Done!\n", r1_count_new, r2_count_new);

	//S1 Migration
	for (i = 0; i < m1_count; i++)
	{
		if (LEVEL >= 2)
			VERBOSE("S X AFTER MIGARTION, START FID [%8"PRId64"]\n", m1[i].fid);
		int64_t total_num = m1[i].total_num;

		int64_t flag = 0; //ok
		int64_t m_chunk_t = 0, m_size_t = 0, d_chunk_t = 0, d_size_t = 0;

		memset(r1_refs, 0, r1_count_new * sizeof(int64_t));

		//buffer the migration file information
		containerid *cc = (containerid*)calloc(1, total_num*sizeof(containerid));
		int64_t *mark = (int64_t*)calloc(1, total_num*sizeof(int64_t));

		for (j = 0; j < total_num; j++)
		{
			//in ==1
			if ((m1[i].arr)[j + total_num] == 1)
			{
				//check in R2
				int64_t mid1 = check_in_r(r2_new, r2_count_new, (m1[i].fps)[j]);
				if (mid1 == -1) //DEL by T2=0
				{
					flag = 1;
					if (LEVEL >= 2)
						VERBOSE("   MIGRATION LOST [%8"PRId64"] in R Y, in = 1, while it (=1) before, but now it lost in r y\n", m1[i].fid);

					break;
				}
				else
				{
					//r1 refs-1
					int64_t tmp = check_in_r(r1_new, r1_count_new, (m1[i].fps)[j]);
					r1_refs[tmp]++;
					//here get the all T=0 chunks, and dedup
					if (r1_new[tmp].refs - r1_refs[tmp] == 0)
					{
						d_chunk_t++;
						d_size_t += (m1[i].arr)[j];
					}
					//cid
					cc[j] = r2_new[mid1].cid;
					mark[j] = 0; //the cid of other
				}
			}

			// find only in myself group
			if ((m1[i].arr)[j + total_num] == 0)
			{
				int64_t tmp = check_in_r(r1_new, r1_count_new, (m1[i].fps)[j]);
				if (tmp == -1) //Del by T1=0
				{
					flag = 1;
					if (LEVEL >= 2)
						VERBOSE("  MIGRATION LOST [%8"PRId64"] in R X, in = 0, while it (=0) brefore, but it lost in r x\n", m1[i].fid);

					break;
				}
				else
				{
					if (r1_refs[tmp] == 0)
					{
						m_chunk_t++;
						m_size_t += (m1[i].arr)[j];
					}

					//R1 refs -1;
					r1_refs[tmp]++;
					mark[j] = 1; //the cid of myself,
					cc[j] = r1_new[tmp].cid;

					//here get the all T=0 chunks, and dedup
					if (r1_new[tmp].refs - r1_refs[tmp] == 0)
					{
						d_chunk_t++;
						d_size_t += (m1[i].arr)[j];
					}
					else if (r1_new[tmp].refs - r1_refs[tmp] < 0)
					{
						VERBOSE("Error\n");
					}

					//r2 add them, do not consider, R2 refs +1
				}
			}
		}

		//the file should be migrated
		if (flag == 0)
		{
			file_count1++;
			m_chunk1 += m_chunk_t;
			m_size1 += m_size_t;
			d_chunk1 += d_chunk_t;
			d_size1 += d_size_t;

			//create a recipe meta
			// new_r_m->fid=ff[i].fid;
			// new_r_m->chunknum=ff[i].chunknum;
			// new_r_m->filesize=ff[i].size;

			//write to new_meta_fp
			//find in mr
			//mr sorted by fid
			int64_t mid2 = check_in_mr(mr1, mr1_count, m1[i].fid);

			if (mid2 == -1)
				VERBOSE("ASSERT NEW MIGRATION RECIPE FAULT [%8"PRId64"], MAP RECIPE NOT FOUND!\n", m1[i].fid);

			char *file_path = db_read_by_int(m1[i].fid);
			if (NULL == file_path) {
				printf("read file:%ld fiailed\n", m1[i].fid);
			}
			uint64_t path_len = strlen(file_path);

			//fprintf(m1_meta_fp, "%"PRId64";%"PRId64";%"PRId64";\n", m1[i].fid, mr1[mid2].chunknum, mr1[mid2].size);
			fprintf(m1_meta_fp, "No. %16"PRId64"; fid= %16"PRId64"; chunknum= %16"PRId64"; size= %16"PRId64"; pathlen=%16"PRId64"; name=%s;\n", i, m1[i].fid, mr1[mid2].chunknum, mr1[mid2].size, path_len, file_path);

			//new recipe
			for (j = 0; j < total_num; j++)
			{
				//append a receipe;
				struct new_recipe *new_r = (struct new_recipe*)malloc(sizeof(struct new_recipe));
				memcpy(new_r->fp, (m1[i].fps)[j], sizeof(fingerprint));
				//0~total for size;
				new_r->size = (m1[i].arr)[j];

				//NOTE THAT r2 new cid, it will be old cid of myself, an create new cid when the migration chunks filled will new container
				new_r->new_cid = cc[j];
				new_r->mark = mark[j];

				if (new_r->mark == 1)
				{
					total_mark1++;
					//VERBOSE("cid=%ld, size=%ld\n",new_r->new_cid,new_r->size);
				}

				//write a new recipe to file
				fwrite(new_r, sizeof(struct new_recipe), 1, m1_recipe_fp);
				// int64_t k;
				// for(k=0;k<20;k++)
				//     fprintf(new_recipe_fp,"%c",((m1[i].fps)[j])[k]);
				// fprintf(new_recipe_fp, ",%"PRId64",%"PRId64"\n", cc[j].cid,(m1[i].sizes)[j]);

				//del the old recipe in recipe fp, we can nonlyu use the re-write
				//TO-DO, how to realize????

				//find mr, mid2, we can locate the offset_m, offset_r
				free(new_r);
			}

			//R1,refs-1
			for (j = 0; j < r1_count_new; j++)
			{
				if (r1_refs[j] != 0)
				{
					r1_new[j].refs -= r1_refs[j];
				}
			}
		}

		free(cc);
		free(mark);
	}

	/*
	if (LEVEL >= 1)
	VERBOSE("\nGROUP MIGRATION DEDUP RESULT: FILE = %" PRId64 ";\nMIGRATION CHUNK = %" PRId64 ", MIGRATION SIZE = %" PRId64 ";\nDEDUP CHUNK = %" PRId64 ", DEDUP SIZE = %" PRId64 "\n", \
	file_count1, m_chunk1, m_size1, d_chunk1, d_size1);
	*/

	stat[0] = file_count1;
	stat[1] = m_chunk1;
	stat[2] = m_size1;
	stat[3] = d_chunk1;
	stat[4] = d_size1;

	free(r1_refs);

	free(r1_new);

	fseek(m1_meta_fp, 0, SEEK_SET);

	fprintf(m1_meta_fp, "TOTAL MIGRATION FILE = %16"PRId64", MARK = %16"PRId64";\n", file_count1, total_mark1);

	fclose(meta1_fp); //these old file fp can be used to del the olf recipe and meta files, but now we do not consider this.
	fclose(recipe1_fp);
	fclose(m1_meta_fp);
	fclose(m1_recipe_fp);

	TIMER_END(1, time_1);
	if (LEVEL >= 0)
	{
		VERBOSE("\n###TOTAL MIGRATION RECIPE WRITE TIME:  %lf (s);\n", time_1);
	}
}