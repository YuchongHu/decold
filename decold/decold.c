#include "dedup.h"
#include "db.h"

extern void load_config();

char path1[100] = "/mnt/disk3/vmdk/trace/8g/g1/";
char path2[100] = "/mnt/disk3/vmdk/trace/8g/g2/";

int enable_migration = 1;
int enable_refs = 0;
int enable_topk = 0;
long int big_file = 0;
float migration_threshold = 0.8;
int db_is_exist = 0;
char db_path[128] = "/tmp/db_path";

//return 1=> B group, -1==> A group, 0==> None Migration
int intersection(const char *path1, const char *path2)
{
	double time_1 = 0, time_2 = 0, time_3 = 0, time_4 = 0;
	TIMER_DECLARE(1);
	TIMER_DECLARE(2);
	TIMER_DECLARE(3);
	TIMER_DECLARE(4);

	TIMER_BEGIN(1);

	struct summary *s1, *s2;
	struct map_recipe *mr1, *mr2;
	int64_t s1_count = 0, s2_count = 0;
	int64_t mr1_count = 0, mr2_count = 0;
	int64_t empty1_count = 0, empty2_count = 0;
	int64_t max1_cid = 0, max2_cid = 0;

	int64_t done_fid1 = read_recipe(0, path1, &s1, &s1_count, &mr1, &mr1_count, &empty1_count, &max1_cid);

	int64_t i, j;
	read_recipe(done_fid1 + 1, path2, &s2, &s2_count, &mr2, &mr2_count, &empty2_count, &max2_cid);

	if (LEVEL >= 1)
	{
		VERBOSE("\n(TOTAL CHUNK) S1_COUNT=%" PRId64 "; (TOTAL FILE) MR1_COUNT=%" PRId64 "; EMPTY FILE = %" PRId64 ";\n", s1_count, mr1_count, empty1_count);
		VERBOSE("(TOTAL CHUNK) S2_COUNT=%" PRId64 "; (TOTAL FILE) MR2_COUNT=%" PRId64 "; EMPTY FILE = %" PRId64 ";\n", s2_count, mr2_count, empty2_count);
	}

	if (enable_topk > 0)
	{
		//top algorithm
		//reduce S1 and S2;
		top1(&s1, &s1_count, &s2, &s2_count, &mr1, &mr1_count, &mr2, &mr2_count);
		VERBOSE("\n(AFTER TOPK CHUNK) S1_COUNT=%" PRId64 "; (AFTER TOTAL FILE) MR1_COUNT=%" PRId64 ";\n", s1_count, mr1_count);
		VERBOSE("(AFTER TOPK CHUNK) S2_COUNT=%" PRId64 "; (AFTER TOTAL FILE) MR2_COUNT=%" PRId64 ";\n", s2_count, mr2_count);
	}

	//statistic the avg groups of one file
	if (LEVEL >= 2)
	{
		int64_t total = total_groups(s1, s1_count, max1_cid);
		VERBOSE("\n\nGROUP 1 TOTAL FILE=%"PRId64"; TOTAL GROUPS=%"PRId64", EACH CHANAGE GROUPS=%lf\n", mr1_count - empty1_count, total, 1.0*total / mr1_count);

		total = total_groups(s2, s2_count, max2_cid);
		VERBOSE("GROUP 2 TOTAL FILE=%"PRId64"; TOTAL GROUPS=%"PRId64", EACH CHANAGE GROUPS=%lf\n\n", mr2_count - empty2_count, total, 1.0*total / mr2_count);
	}

	//copy S1,S2 for migration
	struct summary *s1_c = (struct summary *)malloc(s1_count * sizeof(struct summary));
	struct summary *s2_c = (struct summary *)malloc(s2_count * sizeof(struct summary));
	for (i = 0; i < s1_count; i++)
	{
		s1_c[i] = s1[i];
	}
	for (i = 0; i < s2_count; i++)
	{
		s2_c[i] = s2[i];
	}

	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < s1_count; i++)
		{
			char code[41];
			hash2code(s1[i].fp, code);
			code[40] = 0;
			VERBOSE("S1 No.%"PRId64" [%8" PRId64 "] = %s\n", i, s1_c[i].fid, code);
		}

		VERBOSE("\n----------------------------------------------------------------\n");

		for (i = 0; i < s2_count; i++)
		{
			char code[41];
			hash2code(s2[i].fp, code);
			code[40] = 0;
			VERBOSE("S2 No.%"PRId64" [%8" PRId64 "] = %s\n", i, s2_c[i].fid, code);
		}

		VERBOSE("\n----------------------------------------------------------------\n");
	}

	struct summary_common *scommon1, *scommon2;
	int64_t sc1_count = 0, sc2_count = 0;
	cal_inter(s1, s1_count, s2, s2_count, &scommon1, &sc1_count, &scommon2, &sc2_count);

	struct file_fp *ff1, *ff2;
	int64_t ff1_count = 0, ff2_count = 0;

	struct migrate *m1, *m2;
	int64_t m1_count = 0, m2_count = 0;

	int64_t mig1_count[8] = { 0, 0, 0, 0, 0, 0, 0, 0 }; //60,65,70,75,80,85,90,95%
	int64_t mig2_count[8] = { 0, 0, 0, 0, 0, 0, 0, 0 }; //60,65,70,75,80,85,90,95%

	file_find(mr1, mr1_count, scommon1, sc1_count, &ff1, &ff1_count, &m1, &m1_count, mig1_count);
	file_find(mr2, mr2_count, scommon2, sc2_count, &ff2, &ff2_count, &m2, &m2_count, mig2_count);

	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < ff1_count; i++)
		{
			VERBOSE("DEDU FILES IN S1, No.%" PRId64 ": [%8" PRId64 "], num =% " PRId32 "\n", i, ff1[i].fid, ff1[i].num);
			for (j = 0; j < ff1[i].num; j++)
			{
				char code[41];
				hash2code((ff1[i].fps)[j], code);
				code[40] = 0;
				VERBOSE("   ORDER:(%" PRId64 "), FP= %s\n", j, code);
			}
		}
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < ff2_count; i++)
		{
			VERBOSE("DEDU FILES IN S2, No.%" PRId64 ": [%8" PRId64 "], num =% " PRId32 "\n", i, ff2[i].fid, ff2[i].num);
			for (j = 0; j < ff2[i].num; j++)
			{
				char code[41];
				hash2code((ff2[i].fps)[j], code);
				code[40] = 0;
				VERBOSE("   ORDER:(%" PRId64 "), FP= %s\n", j, code);
			}
		}
		VERBOSE("\n----------------------------------------------------------------\n");
	}

	if (LEVEL >= 1)
		VERBOSE("\nF1 NUM =%" PRId64 ", F2 NUM = %" PRId64 " Done!\n", ff1_count, ff2_count);

	TIMER_BEGIN(2);
	if (enable_migration == 1)
	{
		migration_find(m1, m1_count, s1_c, s1_count);
		migration_find(m2, m2_count, s2_c, s2_count);

		if (LEVEL >= 2)
		{
			VERBOSE("\n----------------------------------------------------------------\n");
			for (i = 0; i < m1_count; i++)
			{
				int64_t total = m1[i].total_num;
				VERBOSE("No.%" PRId64 ", MAY MIGRATE [%8" PRId64 "], TOTAL = %" PRId64 ":\n", i, m1[i].fid, total);

				for (j = 0; j < total; j++)
				{
					if (((m1[i]).arr)[j + total] == 1)
					{
						char code[41];
						hash2code((m1[i].fps)[j], code);
						code[40] = 0;
						VERBOSE("   (%" PRId64 "): {%s}:\n", j, code);
					}
					else
						VERBOSE("   (%" PRId64 "): NOT HERE!\n", j);
				}
			}
			VERBOSE("\n----------------------------------------------------------------\n");

			for (i = 0; i < m2_count; i++)
			{
				int64_t total = m2[i].total_num;
				VERBOSE("No.%" PRId64 ",MAY MIGRATE [%8" PRId64 "], TOTAL = %" PRId64 ":\n", i, m2[i].fid, total);

				for (j = 0; j < total; j++)
				{
					if (((m2[i]).arr)[j + total] == 1)
					{
						char code[41];
						hash2code((m2[i].fps)[j], code);
						code[40] = 0;
						VERBOSE("   (%" PRId64 "): {%s}:\n", j, code);
					}
					else
						VERBOSE("   (%" PRId64 "): NOT HERE!\n", j);
				}
			}
			VERBOSE("\n----------------------------------------------------------------\n");
		}

		if (LEVEL >= 1)
		{
			VERBOSE("\nMIGRATION FILE NUM =%" PRId64 ", MIGRATION FILE NUM = %" PRId64 " Done!\n", m1_count, m2_count);
			VERBOSE("\nGROUP 1 ==> \n[0.6]: %"PRId64"; [0.65]: %"PRId64"; [0.7]: %"PRId64"; [0.75]: %"PRId64"; [0.8]: %"PRId64"; [0.85]: %"PRId64"; [0.9]: %"PRId64"; [0.95]: %"PRId64";\n", \
				mig1_count[0], mig1_count[1], mig1_count[2], mig1_count[3], mig1_count[4], mig1_count[5], mig1_count[6], mig1_count[7]);
			VERBOSE("\nGROUP 2 ==> \n[0.6]: %"PRId64"; [0.65]: %"PRId64"; [0.7]: %"PRId64"; [0.75]: %"PRId64"; [0.8]: %"PRId64"; [0.85]: %"PRId64"; [0.9]: %"PRId64"; [0.95]: %"PRId64";\n", \
				mig2_count[0], mig2_count[1], mig2_count[2], mig2_count[3], mig2_count[4], mig2_count[5], mig2_count[6], mig2_count[7]);
		}
	}
	TIMER_END(2, time_2);

	//free sc
	free(s1_c);
	free(s2_c);

	struct target *r1, *r2;
	int64_t r1_count = 0, r2_count = 0;

	//R1,R2 for all refs
	//to T1,T2
	all_refs_find(s1, s1_count, &r1, &r1_count);
	all_refs_find(s2, s2_count, &r2, &r2_count);

	//sorted by fp
	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < r1_count; i++)
		{
			char code[41];
			hash2code(r1[i].fp, code);
			code[40] = 0;
			VERBOSE("No.%" PRId64 ", R1 refs={%" PRId32 "}, fp= %s\n", i, r1[i].refs, code);
		}
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < r2_count; i++)
		{
			char code[41];
			hash2code(r2[i].fp, code);
			code[40] = 0;
			VERBOSE("No.%" PRId64 ", R2 refs={%" PRId32 "}, fp= %s\n", i, r2[i].refs, code);
		}
		VERBOSE("\n----------------------------------------------------------------\n");
	}

	if (LEVEL >= 1)
		VERBOSE("\nR1(%" PRId64 ") and R2(%" PRId64 ") Done!\n", r1_count, r2_count);

	//T1,T2
	struct target *t1, *t2;
	int64_t t1_count = 0, t2_count = 0;

	target_refs_find(r1, r1_count, &t1, &t1_count, ff1, ff1_count);
	target_refs_find(r2, r2_count, &t2, &t2_count, ff2, ff2_count);

	if (enable_refs == 0)
	{
		if (LEVEL >= 2)
		{
			VERBOSE("\n----------------------------------------------------------------\n");
			for (i = 0; i < t1_count; i++)
			{
				char code[41];
				hash2code(t1[i].fp, code);
				code[40] = 0;
				VERBOSE("No.%" PRId64 ", T1 ALL, refs={%" PRId32 "}, fp= %s\n", i, t1[i].refs, code);
			}
			VERBOSE("\n----------------------------------------------------------------\n");
			for (i = 0; i < t2_count; i++)
			{
				char code[41];
				hash2code(t2[i].fp, code);
				code[40] = 0;
				VERBOSE("No.%" PRId64 ", T2 ALL refs={%" PRId32 "}, fp= %s\n", i, t2[i].refs, code);
			}
			VERBOSE("\n----------------------------------------------------------------\n");
		}
		if (LEVEL >= 1)
			VERBOSE("\nT1(%" PRId64 ") and T2(%" PRId64 ") ALL DEDUP Done!\n", t1_count, t2_count);
	}
	else
	{
		if (LEVEL >= 2)
		{
			VERBOSE("\n----------------------------------------------------------------\n");
			for (i = 0; i < t1_count; i++)
			{
				char code[41];
				hash2code(t1[i].fp, code);
				code[40] = 0;
				VERBOSE("No.%" PRId64 ", T1 ONLY >1, refs={%" PRId32 "}, fp= %s\n", i, t1[i].refs, code);
			}
			VERBOSE("\n----------------------------------------------------------------\n");
			for (i = 0; i < t2_count; i++)
			{
				char code[41];
				hash2code(t2[i].fp, code);
				code[40] = 0;
				VERBOSE("No.%" PRId64 ", T2 ONLY >1, refs={%" PRId32 "}, fp= %s\n", i, t2[i].refs, code);
			}
			VERBOSE("\n----------------------------------------------------------------\n");
		}
		if (LEVEL >= 1)
			VERBOSE("\nT1(%" PRId64 ") and T2(%" PRId64 ") ONLY >1 DEDUP Done!\n", t1_count, t2_count);
	}

	//T1 ^ T2
	// fingerprint *t1_2;
	// int64_t t1_2_count = 0;
	// inter_t1_t2(&t1_2, &t1_2_count, t1, t1_count, t2, t2_count);
	// free(t1_2);

	//stat_max_dedup
	fingerprint *dedup1, *dedup2;
	int64_t d1_count = 0, d2_count = 0;

	if (enable_refs == 0)
		stat_max_dedup(&dedup1, &d1_count, &dedup2, &d2_count, ff1, ff1_count, t1, t1_count, ff2, ff2_count, t2, t2_count);
	else
		stat_max_dedup_ignore(&dedup1, &d1_count, &dedup2, &d2_count, ff1, ff1_count, t1, t1_count, ff2, ff2_count, t2, t2_count, r1_count, r2_count);

	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < d1_count; i++)
		{
			char code[41];
			hash2code(dedup1[i], code);
			code[40] = 0;
			VERBOSE("No.%d, DEDUP1 =0 & REFS = 1,fp= %s\n", i, code);
		}
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < d2_count; i++)
		{
			char code[41];
			hash2code(dedup2[i], code);
			code[40] = 0;
			VERBOSE("No.%d, DEDUP2 =0 & REFS = 1,fp= %s\n", i, code);
		}
		VERBOSE("\n----------------------------------------------------------------\n");
	}

	if (LEVEL >= 0)
		VERBOSE("\nMAX_DEDUP of DEL ALL S1, CHUNK(%" PRId64 " + %" PRId64 " = %" PRId64 ")\n", d1_count, d2_count, d1_count + d2_count);

	int64_t stat1[2] = { 0, 0 }, stat2[2] = { 0, 0 };
	double time_c1 = 0, time_c2 = 0;
	//create intersection recipe, S1 need t2
	TIMER_BEGIN(2);
	time_c1 = create_recipe(path1, dedup1, d1_count, ff1, ff1_count, r2, r2_count, mr1, mr1_count, stat1);
	TIMER_END(2, time_2);
	TIMER_BEGIN(3);
	time_c2 = create_recipe(path2, dedup2, d2_count, ff2, ff2_count, r1, r1_count, mr2, mr2_count, stat2);
	TIMER_END(3, time_3);

	if (LEVEL >= 0)
	{
		VERBOSE("\n###GROUP CREATE RECIPE: %lf (s), %lf (s), TOTAL TIME: %lf (s);\n", time_c1, time_c2, time_c1 + time_c2);
	}

	if (LEVEL >= 0)
	{
		VERBOSE("\n\nGROUP 1 DEDUP RESULT:    FILE = %" PRId64 ", CHUNK = %" PRId64 ", SIZE = %" PRId64 " (B);\n", stat1[0], d1_count, stat1[1]);
		VERBOSE("GROUP 2 DEDUP RESULT:    FILE  = %" PRId64 ", CHUNK = %" PRId64 ", SIZE = %" PRId64 " (B);\n", stat2[0], d2_count, stat2[1]);
		VERBOSE("TOTAL DEDUP RESULT:  FILE = %" PRId64 ", CHUNK = %" PRId64 ", SIZE = %" PRId64 " (B);\n", stat1[0] + stat2[0], d1_count + d2_count, stat1[1] + stat2[1]);
		VERBOSE("TOTAL INTERSECTION TIME:  %lf (s) + %lf (s) = %lf (s);\n\n", time_2, time_3, time_2 + time_3);
	}

	int64_t stat3[5] = { 0, 0, 0, 0, 0 };
	if (enable_migration == 1)
	{
		TIMER_BEGIN(4);
		//Migration
		//choose one group migrate to other, choose a better group

		//m1 group to m2 group
		if (m1_count > m2_count)
			create_migartion(path1, m1, m1_count, r1, r1_count, r2, r2_count, dedup1, d1_count, dedup2, d2_count, mr1, mr1_count, stat3);
		else
			create_migartion(path2, m2, m2_count, r2, r2_count, r1, r1_count, dedup2, d2_count, dedup1, d1_count, mr2, mr2_count, stat3);

		TIMER_END(4, time_4);
		if (LEVEL >= 0)
		{
			VERBOSE("\n\nTOTAL MIGRATION DEDUP RESULT:\nFILE = %" PRId64 ";\nMIGRATION CHUNK = %" PRId64 ", MIGRATION SIZE = %" PRId64 " (B);\nDEDUP CHUNK = %" PRId64 ", DEDUP SIZE = %" PRId64 "(B)\n", \
				stat3[0], stat3[1], stat3[2], stat3[3], stat3[4]);
			VERBOSE("TOTAL MIGRATION TIME:  %lf (s);\n\n", time_4);
		}
	}

	TIMER_END(1, time_1);
	if (LEVEL >= 0)
	{
		VERBOSE("\n\n###TOTAL MIGRATE:  %"PRId64" (B);\nTOTAL DEDUP:  %"PRId64" (B);\nTOTAL TIME:  %lf (s);\n\n", stat3[2], stat1[1] + stat2[1] + stat3[4], time_1);
	}

	//free m1,m2;
	for (i = 0; i < m1_count; i++)
	{
		free(m1[i].fps);
		// free(m1[i].sizes);
		// free(m1[i].in);
		free(m1[i].arr);
	}

	//free dedup1,2
	free(dedup1); free(dedup2);

	//free R1, R2
	free(r1); free(r2);

	//free t1,t2
	free(t1); free(t2);

	//free ff1;ff2
	for (i = 0; i < ff1_count; i++)
	{
		free(ff1[i].fps);
		free(ff1[i].sizes);
	}
	for (i = 0; i < ff2_count; i++)
	{
		free(ff2[i].fps);
		free(ff2[i].sizes);
	}

	//free s1,mr1,mr2
	free(s1); free(mr1);
	free(s2); free(mr2);

	if (enable_migration == 1)
		return m1_count > m2_count ? 1 : -1;
	else
		return 0;
}

static int comp_containerid(const void *nm1, const void *nm2)
{
	return ((struct new_migration*)nm1)->old_cid - ((struct new_migration*)nm2)->old_cid;
}

//when the old group is power on, we should read data from its pool
//from the pool name, we got the group that contains the container
void start_migrate_data(const char *path)
{
	double time_1 = 0;

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	char path_meta[100], path_recipe[100], path_pool[100], new_path_pool[100];

	sprintf(path_meta, "%s%s%s", path, "migrate_group", ".meta");
	sprintf(path_recipe, "%s%s%s", path, "migrate_group", ".recipe");
	sprintf(path_pool, "%s%s", path, "container.pool");
	sprintf(new_path_pool, "%s%s", path, "new.pool");

	if ((0 != access(path_meta, F_OK)) || (0 != access(path_recipe, F_OK)))
	{
		printf("%s or %s not exist\n", path_meta, path_recipe);
		return;
	}
	//printf("%s;\n%s;\n%s\n",path_meta,path_pool,new_path_pool);

	FILE *meta_fp = fopen(path_meta, "r");
	FILE *recipe_fp = fopen(path_recipe, "r");
	FILE *pool_fp = fopen(path_pool, "r");
	FILE *new_pool_fp = fopen(new_path_pool, "w+");

	if (!(meta_fp && recipe_fp && pool_fp && new_pool_fp))
		perror("\nMIGRATE FILE OPEN FAULT!\n");

	char line[1024];
	int64_t total_file = 0, total_mark = 0;

	fgets(line, 100, meta_fp);
	sscanf(line, "TOTAL MIGRATION FILE = %16"PRId64", MARK = %16"PRId64";\n", &total_file, &total_mark);
	// VERBOSE("LINE=%s\n",line);

	struct new_recipe *nr = (struct new_recipe*)malloc(1 * sizeof(struct new_recipe));

	//for locality
	struct new_migration *nm = (struct new_migration*)malloc(total_mark*sizeof(struct new_migration));
	int64_t nm_count = 0;

	VERBOSE("total file= %d\n", total_file);

	int64_t i, j;
	for (i = 0; i < total_file; i++)
	{
		//read migrate recipe, new meta and recipe are done
		int64_t fid = 0;
		int64_t chunknum = 0; //chunknum;
		int64_t filesize = 0;

		fgets(line, 1024, meta_fp);
		//VERBOSE("LINE=%s\n",line);
		int64_t tmp;

		sscanf(line, "No. %"PRId64"; fid= %"PRId64"; chunknum= %"PRId64"; size= %"PRId64";", &tmp, &fid, &chunknum, &filesize);

		//read recipe files
		for (j = 0; j < chunknum; j++)
		{
			// fread(&(nr->fp), sizeof(fingerprint), 1, recipe_fp);
			// fread(&(nr->size), sizeof(int64_t), 1, recipe_fp);
			// fread(&(nr->mark), sizeof(int64_t), 1, recipe_fp);
			// fread(&(nr->new_cid), sizeof(int64_t), 1, recipe_fp);

			fread(nr, sizeof(struct new_recipe), 1, recipe_fp);

			if (nr->mark == 1)
			{
				//[i][j];
				memcpy(nm[nm_count].fp, nr->fp, sizeof(fingerprint));
				nm[nm_count].file_no = i;
				nm[nm_count].chunk_no = j;
				nm[nm_count].size = nr->size;
				nm[nm_count].old_cid = nr->new_cid; //retireve from old_cid
				nm[nm_count].data = (unsigned char*)malloc(nr->size * sizeof(unsigned char));
				nm_count++;
				//VERBOSE("size=%u; mark=%u; new_cid=%"PRId64";\n",nr->size,nr->mark,nr->new_cid);
			}
		}
	}

	// sort by old_cid
	qsort(nm, total_mark, sizeof(struct new_migration), comp_containerid);

	//for(i=0;i<nm_count;i++)
	//    VERBOSE("cid=%"PRId64",%d, %d, %"PRId32"\n", nm[i].old_cid,nm[i].file_no,nm[i].chunk_no,nm[i].size);

	for (i = 0; i < nm_count;)
	{
		int64_t num = 1;
		while (nm[i].old_cid == nm[i + num].old_cid)
			num++;

		unsigned char **v = (unsigned char **)malloc(num* sizeof(unsigned char*));
		fingerprint *fps = (fingerprint*)malloc(num* sizeof(fingerprint));

		for (j = 0; j < num; j++)
		{
			v[j] = (unsigned char*)malloc(nm[i + j].size * sizeof(unsigned char));
			memcpy(fps[j], nm[i + j].fp, sizeof(fingerprint));
		}

		//locality
		retrieve_from_container(pool_fp, nm[i].old_cid, v, fps, num);

		i = i + num;

		free(fps);
		for (j = 0; j < num; j++)
		{
			free(v[j]);
		}
		free(v);
	}

	//write to disk, for simplicity
	for (i = 0; i < nm_count; i++)
	{
		fprintf(new_pool_fp, "No.file=%"PRId64", No.chunk=%"PRId64", size=%"PRId64", old_cid=%"PRId64"\n", nm[i].file_no, nm[i].chunk_no, nm[i].size, nm[i].old_cid);
		if (nm[i].fp && nm[i].data)
		{
			fwrite(nm[i].fp, sizeof(fingerprint), 1, new_pool_fp);
			fwrite(nm[i].data, nm[i].size*sizeof(unsigned char), 1, new_pool_fp);
		}
		else
			VERBOSE("ASSERT, nm struct fault!\n");
	}

	fclose(meta_fp);
	fclose(recipe_fp);
	fclose(pool_fp);
	fclose(new_pool_fp);

	TIMER_END(1, time_1);
	if (LEVEL >= 1)
	{
		VERBOSE("\n###GROUP DISK RETRIEVE MIGRATE TIME:  %lf (s);\n", time_1);
	}
}

int construct_rocksdb(const char *path1, const char *path2)
{
	double db_construction_time = 0;
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	if (0 != db_init(db_path, 1)) {
		printf("create db:%s error\n", db_path);
		return -1;
	}

	if (db_is_exist) {
		printf("db exist, skip\n");
		return 0;
	}

	int64_t done_fid1 = construct_db(0, path1);
	if (done_fid1 < 0) {
		printf("construct %s db fail\n", path1);
		return -1;
	}
	if (construct_db(done_fid1 + 1, path2) < 0) {
		printf("construct %s db fail\n", path1);
		return -1;
	}
	TIMER_END(1, db_construction_time);
	VERBOSE("\n###ROCKSDB CONSTRUCT TIME:  %lf (s);\n", db_construction_time);

	return 0;
}

// read out for test and not finsh the job
void restore_migration(const char* path, char *path_opposite)
{
	double time_1 = 0;
	int batch = 100;

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	char path_meta[100], path_recipe[100], opposite_path_pool[100], new_path_pool[100];

	sprintf(path_meta, "%s%s%s", path, "migrate_group", ".meta");
	sprintf(path_recipe, "%s%s%s", path, "migrate_group", ".recipe");
	sprintf(opposite_path_pool, "%s%s", path_opposite, "container.pool");
	sprintf(new_path_pool, "%s%s", path, "new.pool");

	if ((0 != access(path_meta, F_OK)) || (0 != access(path_recipe, F_OK)))
	{
		printf("%s or %s not exist\n", path_meta, path_recipe);
		return;
	}

	//printf("%s;\n%s;\n%s\n",path_meta,opposite_path_pool,new_path_pool);

	FILE *meta_fp = fopen(path_meta, "r");
	FILE *recipe_fp = fopen(path_recipe, "r");
	FILE *pool_fp = fopen(opposite_path_pool, "r");
	FILE *new_pool_fp = fopen(new_path_pool, "r");

	if (!(meta_fp && recipe_fp && pool_fp && new_pool_fp))
		perror("\nREAD MIGRATE FILE OPEN FAULT!\n");

	char line[1024];
	int64_t total_file = 0, total_mark = 0;

	fgets(line, 100, meta_fp);
	sscanf(line, "TOTAL MIGRATION FILE = %16"PRId64", MARK = %16"PRId64";\n", &total_file, &total_mark);
	// VERBOSE("LINE=%s\n",line);

	//batch for 100
	struct new_migration *old = (struct new_migration*)malloc(batch*sizeof(struct new_migration));
	int64_t old_count = 0;
	struct new_recipe *nr = (struct new_recipe*)malloc(1 * sizeof(struct new_recipe));

	int64_t i, j;
	for (i = 0; i < total_file; i++)
	{
		//read migrate recipe, new meta and recipe are done
		int64_t fid = 0;
		int64_t chunknum = 0; //chunknum;
		int64_t filesize = 0;

		fgets(line, 1024, meta_fp);
		//VERBOSE("LINE=%s\n",line);
		int64_t tmp;

		sscanf(line, "No. %"PRId64"; fid= %"PRId64"; chunknum= %"PRId64"; size= %"PRId64";", &tmp, &fid, &chunknum, &filesize);

		//read recipe files
		for (j = 0; j < chunknum; j++)
		{
			// fread(&(nr->fp), sizeof(fingerprint), 1, recipe_fp);
			// fread(&(nr->size), sizeof(int64_t), 1, recipe_fp);
			// fread(&(nr->mark), sizeof(int64_t), 1, recipe_fp);
			// fread(&(nr->new_cid), sizeof(int64_t), 1, recipe_fp);

			fread(nr, sizeof(struct new_recipe), 1, recipe_fp);

			//read from old container
			if (nr->mark == 0)
			{
				//retrieve_from_container_demo(pool_fp, nr->new_cid, &(old[old_count].data), &(nr->fp));

				//[i][j];
				memcpy(old[old_count].fp, nr->fp, sizeof(fingerprint));
				old[old_count].file_no = i;
				old[old_count].chunk_no = j;
				old[old_count].size = nr->size;
				old[old_count].old_cid = nr->new_cid; //retireve from old_cid
				old[old_count].data = (unsigned char*)malloc(nr->size * sizeof(unsigned char));
				old_count++;
				//VERBOSE("size=%u; mark=%u; new_cid=%"PRId64";\n",nr->size,nr->mark,nr->new_cid);

				if (old_count == batch)
				{
					// sort by old_cid
					qsort(old, old_count, sizeof(struct new_migration), comp_containerid);
					int a, b;
					for (a = 0; a < old_count;)
					{
						int64_t num = 1;
						while (old[a].old_cid == old[a + num].old_cid)
							num++;

						unsigned char **v = (unsigned char **)malloc(num* sizeof(unsigned char*));
						fingerprint *fps = (fingerprint*)malloc(num* sizeof(fingerprint));

						for (b = 0; b < num; b++)
						{
							v[b] = (unsigned char*)malloc(old[a + b].size * sizeof(unsigned char));
							memcpy(fps[b], old[a + b].fp, sizeof(fingerprint));
						}

						//locality
						retrieve_from_container(pool_fp, old[a].old_cid, v, fps, num);

						a = a + num;

						free(fps);
						for (b = 0; b < num; b++)
						{
							free(v[b]);
						}
						free(v);
					}

					int d = 0;
					for (d = 0; d < old_count; d++)
						free(old[d].data);
					old_count = 0;
				}
			}

			//read from new pool
			if (nr->mark == 1)
			{
				struct new_migration tmp;
				//for No.file
				fgets(line, 1024, new_pool_fp);
				sscanf(line, "No.file=%"PRId64", No.chunk=%"PRId64", size=%"PRId64", old_cid=%"PRId64"", &(tmp.file_no), &(tmp.chunk_no), &(tmp.size), &(tmp.old_cid));

				fingerprint fp_tmp;
				fread(fp_tmp, sizeof(fingerprint), 1, new_pool_fp);

				unsigned char* p = (unsigned char*)malloc(tmp.size*sizeof(unsigned char));
				fread(p, tmp.size*sizeof(unsigned char), 1, new_pool_fp);
				free(p);

				//print_unsigned(p,old[i].size);
			}
		}
	}

	fclose(meta_fp);
	fclose(recipe_fp);
	fclose(pool_fp);
	fclose(new_pool_fp);

	TIMER_END(1, time_1);
	if (LEVEL >= 1)
	{
		VERBOSE("\n###READ RESTORE TIME:  %lf (s) ;\n", time_1);
	}
}

int main(int argc, char *argv[])
{
	if (3 != argc) {
		printf("usage: ./decold path1 path2\n");
		return -1;
	}

	printf("path1=%s\npath2=%s\n", argv[1], argv[2]);

	load_config();

	if (0 != construct_rocksdb(argv[1], argv[2])) {
		printf("construct db failed");
		return -1;
	}

	int a = intersection(argv[1], argv[2]);
	if (a == 1)
	{
		start_migrate_data(argv[1]);
		//restore append in the group to associate Destor
		restore_migration(argv[1], argv[2]);
	}
	if (a == -1)
	{
		start_migrate_data(argv[2]);
		//restore append in the group to associate Destor
		restore_migration(argv[2], argv[1]);
	}
	return 0;
}