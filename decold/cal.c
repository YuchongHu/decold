#include "cal.h"

int64_t MIGRATION_COUNT = 0;
extern int enable_migration;
extern int enable_refs;
extern int enable_topk;
extern long int big_file;
extern float migration_threshold;

static int comp(const void *s1, const void *s2)
{
	return comp_code(((struct summary *)s1)->fp, ((struct summary *)s2)->fp);
}

static int comp_scommon(const void *s1, const void *s2)
{
	return ((struct summary_common *)s1)->fid - ((struct summary_common *)s2)->fid;
}

// //MAY HAVE SOME SAME CHUNK
// static int comp_s1(const void *s1, const void *s2)
// {
//     return ((struct summary_double *)s1)->fid_s1 - ((struct summary_double *)s2)->fid_s1;
// }

// //MAY HAVE SOME SAME CHUNK
// static int comp_s2(const void *s1, const void *s2)
// {
//     return ((struct summary_double *)s1)->fid_s2 - ((struct summary_double *)s2)->fid_s2;
// }

static int comp_mr(const void *mr1, const void *mr2)
{
	return ((struct map_recipe *)mr1)->fid - ((struct map_recipe *)mr2)->fid;
}

// static size_t comp_fp(const void *fp1, const void *fp2)
// {
//     return comp_code((unsigned char*)fp1,(unsigned char*)fp2);
// }

static int comp_tp1(const void *tp1, const void *tp2)
{
	return comp_code(((struct top1*)tp1)->fp, ((struct top1*)tp2)->fp);
}

// static int comp_tpk(const void *tp1, const void *tp2)
// {
//     return comp_code((((struct top*)tp1)->fps)[0],(((struct top*)tp2)->fps)[0]);
// }

//we copy another S1 S2 for R1 and migartion sort
void cal_inter(struct summary *s1, int64_t s1_count, struct summary *s2, int64_t s2_count, struct summary_common **scommon1, int64_t *sc1_count, struct summary_common **scommon2, int64_t *sc2_count)
{
	//sorted s1
	qsort(s1, s1_count, sizeof(struct summary), comp);

	qsort(s2, s2_count, sizeof(struct summary), comp);

	int64_t i;
	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < s1_count; i++)
		{
			char code[41];
			hash2code(s1[i].fp, code);
			code[40] = 0;
			VERBOSE("SORTED S1 No.%" PRId64 " [%8" PRId64 "], order=%"PRId64", size=%"PRId64", containerid=%"PRId64", code = %s\n", \
				i, s1[i].fid, s1[i].order, s1[i].size, s1[i].cid, code);
		}

		VERBOSE("\n----------------------------------------------------------------\n");

		for (i = 0; i < s2_count; i++)
		{
			char code[41];
			hash2code(s2[i].fp, code);
			code[40] = 0;
			VERBOSE("SORTED S2 No.%" PRId64 " [%8" PRId64 "], order=%"PRId64", size=%"PRId64", containerid=%"PRId64", code = %s\n", \
				i, s2[i].fid, s2[i].order, s2[i].size, s2[i].cid, code);
		}

		VERBOSE("\n----------------------------------------------------------------\n");
	}

	//malloc for the scommon
	struct summary_common *scommon11 = (struct summary_common *)malloc(sizeof(struct summary_common) * s1_count);
	struct summary_common *scommon22 = (struct summary_common *)malloc(sizeof(struct summary_common) * s2_count);

	int64_t a = 0, b = 0;
	while (a < s1_count && b < s2_count)
	{
		if (comp_code(s1[a].fp, s2[b].fp) == 0)
		{
			//It can consider re-writer, we think fps will set in the same container

			int64_t c = 1, d = 1;
			//check that c has some same file
			while (a + c < s1_count && comp_code(s1[a].fp, s1[a + c].fp) == 0)
			{
				c++;
			}
			//check that d has some same file.

			while (b + d < s2_count && comp_code(s2[b].fp, s2[b + d].fp) == 0)
			{
				d++;
			}

			for (i = 0; i < c; i++)
			{
				memcpy(scommon11[*sc1_count].fp, s1[a + i].fp, sizeof(fingerprint));
				scommon11[*sc1_count].size = s1[a + i].size;

				scommon11[*sc1_count].cid = s1[a + i].cid;
				scommon11[*sc1_count].fid = s1[a + i].fid;
				scommon11[*sc1_count].order = s1[a + i].order;
				(*sc1_count)++;
			}

			for (i = 0; i < d; i++)
			{
				memcpy(scommon22[*sc2_count].fp, s2[b + i].fp, sizeof(fingerprint));
				scommon22[*sc2_count].size = s2[b + i].size;

				scommon22[*sc2_count].cid = s2[b + i].cid;
				scommon22[*sc2_count].fid = s2[b + i].fid;
				scommon22[*sc2_count].order = s2[b + i].order;
				(*sc2_count)++;
			}

			a += c;
			b += d;
			// char code[41];
			// hash2code(s1_2[*s1_2_count-1].fp, code);
			// code[40] = 0;
			// VERBOSE("No.%" PRId64 " [%8" PRId64 "][%8" PRId64 "] = %s\n", *s1_2_count-1, s1[a-1].fid, s2[b-1].fid, code);
		}
		else if (comp_code(s1[a].fp, s2[b].fp) > 0)
		{
			b++;
		}
		else
		{
			a++;
		}
	}

	//show struct summary_common
	//sorted by fps
	scommon11 = (struct summary_common *)realloc(scommon11, *sc1_count * sizeof(struct summary_common));
	scommon22 = (struct summary_common *)realloc(scommon22, *sc2_count * sizeof(struct summary_common));

	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < *sc1_count; i++)
		{
			char code[41];
			hash2code(scommon11[i].fp, code);
			code[40] = 0;
			VERBOSE("SUMMARY COMMON 1 No.% size=%" PRId64 ", containerid=%" PRId64 ", [%8" PRId64 "](%" PRId64 ");\n", \
				i, scommon11[i].size, scommon11[i].cid, scommon11[i].fid, scommon11[i].order, code);
		}
		for (i = 0; i < *sc2_count; i++)
		{
			char code[41];
			hash2code(scommon22[i].fp, code);
			code[40] = 0;
			VERBOSE("SUMMARY COMMON 2 No.%d size=%" PRId64 ", containerid=%" PRId64 ", [%8" PRId64 "](%" PRId64 ");\n", \
				i, scommon22[i].size, scommon22[i].cid, scommon22[i].fid, scommon22[i].order, code);
		}

		/*
		for (i = 0; i < *s1_2_count; i++)
		{
		char code[41];
		hash2code(s1_2[i].fp, code);
		code[40] = 0;
		VERBOSE("SUMMARY COMMON No.%" PRId64 " size=%" PRId64 ", containerid=%" PRId64 ", [%8" PRId64 "](%" PRId64 ");    [%8" PRId64 "](%" PRId64 ") = %s\n", \
		i, s1_2[i].size, s1_2[i].cid, s1_2[i].fid_s1, s1_2[i].order1, s1_2[i].fid_s2, s1_2[i].order2, code);
		}
		*/

		VERBOSE("\n----------------------------------------------------------------\n");
	}

	if (LEVEL >= 1)
		VERBOSE("\nTOTAL SUMMARY COMMON 1 =% "PRId64";\nTOTAL SUMMARY COMMON 2 =% "PRId64";", *sc1_count, *sc2_count);

	*scommon1 = scommon11;
	*scommon2 = scommon22;
}

//check the full chunks in the S common
//0 none and migration < shreshold, <0 ->migration, >0 intersection
//if the bigger file, threshold smaller
static int64_t file_dedup_migration(struct map_recipe *mr, int64_t count, int64_t fid, int64_t chunknum, int64_t mig_count[8])
{
	//binarySearch
	int64_t begin = 0, end = count - 1;
	while (begin <= end)
	{
		int64_t mid = (begin + end) / 2;
		if (mr[mid].fid == fid)
		{
			//TO-DO here for miagration!!!
			if (mr[mid].chunknum == chunknum)
				return mr[mid].chunknum; //>0 intersection
			else
			{
				//only big file migration
				if (enable_migration == 1)
				{
					double ratio = 1.0 * chunknum / mr[mid].chunknum;
					if (mr[mid].size >= big_file && ratio >= migration_threshold)
					{
						if (LEVEL >= 2)
							VERBOSE("BIG MIGRATION NO.%" PRId64 ", [%8" PRId64 "](size=%"PRId64"), { get [%" PRId64 "] of [%" PRId64 "] = %lf.} ==> MAY BE MIGRATION!\n", \
							MIGRATION_COUNT++, fid, mr[mid].size, chunknum, mr[mid].chunknum, ratio);

						if (ratio >= 0.6)
						{
							mig_count[0]++;
							if (ratio >= 0.65)
							{
								mig_count[1]++;
								if (ratio >= 0.7)
								{
									mig_count[2]++;
									if (ratio >= 0.75)
									{
										mig_count[3]++;
										if (ratio >= 0.8)
										{
											mig_count[4]++;
											if (ratio >= 0.85)
											{
												mig_count[5]++;
												if (ratio >= 0.9)
												{
													mig_count[6]++;
													if (ratio >= 0.95)
													{
														mig_count[7]++;
													}
												}
											}
										}
									}
								}
							}
						}

						return 0 - mr[mid].chunknum; //mark for migration
					}

					/*
					if (mr[mid].size < BIG_FILE && 1.0 * chunknum / mr[mid].chunknum >= SMALL_MIGRATION_THRESHOLD)
					{
					if (LEVEL >= 2)
					VERBOSE("SMALL MIGRATION NO.%" PRId32 ", [%8" PRId64 "](size=), { get [%" PRId32 "] of [%" PRId32 "] = %lf.} ==> MAY BE MIGRATION!\n", \
					MIGRATION_COUNT++, fid, mr[mid].size, chunknum, mr[mid].chunknum, 1.0 * chunknum / mr[mid].chunknum);

					return 0 - mr[mid].chunknum; //mark for migration
					}
					*/
				}

				return 0; //migration < shreshold
			}
		}
		else if (mr[mid].fid < fid)
			begin = mid + 1;
		else
			end = mid - 1;
	}

	return 0; //none
}

void file_find(struct map_recipe *mr, int64_t mr_count, struct summary_common *scommon, int64_t sc_count, struct file_fp **fff, int64_t *ff_count, struct migrate **mm, int64_t *m_count, int64_t mig_count[8])
{
	int64_t i;
	//sort mr1 by fid
	qsort(mr, mr_count, sizeof(struct map_recipe), comp_mr);

	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		//show mr1
		for (i = 0; i < mr_count; i++)
		{
			VERBOSE("SORT MAP RECIPE: [%8" PRId64 "],num=%" PRId64 ",size=%" PRId64 ";offset_m=%" PRId64 ";offset_r=%" PRId64 ";\n", mr[i].fid, mr[i].chunknum, \
				mr[i].size, mr[i].offset_m, mr[i].offset_r);
			/*
			VERBOSE("SORT MAP RECIPE: [%8" PRId64 "],num=%" PRId64 ",size=%" PRId64 ";\n", mr[i].fid, mr[i].chunknum, \
			mr[i].size);
			*/
		}
		VERBOSE("\n----------------------------------------------------------------\n");
	}

	struct file_fp *ff = (struct file_fp *)malloc(sc_count * sizeof(struct file_fp));
	struct migrate *m = (struct migrate *)malloc(sc_count * sizeof(struct migrate));

	//sort ccommon, look for files in S

	//sorted ccommon by fid
	qsort(scommon, sc_count, sizeof(struct summary_common), comp_scommon);
	for (i = 0; i < sc_count;)
	{
		int64_t j = 1;
		while (i + j < sc_count && scommon[i].fid == scommon[i + j].fid)
			j++;

		//all chunk = chunksize of the file
		int64_t k = file_dedup_migration(mr, mr_count, scommon[i].fid, j, mig_count);
		if (k > 0)
		{
			//put the files in the common file
			ff[*ff_count].fid = scommon[i].fid;
			ff[*ff_count].num = k;
			ff[*ff_count].fps = (fingerprint *)malloc(k * sizeof(fingerprint));
			ff[*ff_count].sizes = (int64_t *)malloc(k * sizeof(int64_t));
			//ff[*ff_count].cids = (int64_t *)malloc(k * sizeof(int64_t));

			int64_t s;
			for (s = 0; s < k; s++)
			{
				//fps are sorted by order
				int64_t order = scommon[i + s].order;
				memcpy((ff[*ff_count].fps)[order], scommon[i + s].fp, sizeof(fingerprint));
				(ff[*ff_count].sizes)[order] = scommon[i + s].size;
				//cid
				//(ff[*ff_count].cids)[order] = s1_2[i + s].cid;
			}

			(*ff_count)++;
		}

		if (enable_migration == 1 && k < 0) //migration
		{
			int64_t total_num = (int64_t)(0 - k);
			//put the files in the migarion buffer
			m[*m_count].fid = scommon[i].fid;
			m[*m_count].total_num = total_num;
			m[*m_count].fps = (fingerprint *)malloc(total_num * sizeof(fingerprint));
			m[*m_count].arr = (int64_t *)calloc(1, 2 * total_num * sizeof(int64_t));
			//m[*m_count].in = (int64_t *)calloc(1, total_num * sizeof(int64_t));
			// m[*m_count].cids = (int64_t *)calloc(1, total_num * sizeof(int64_t));

			int64_t s;
			for (s = 0; s < j; s++)
			{
				//fps are sorted by order
				int64_t order = scommon[i + s].order;

				memcpy((m[*m_count].fps)[order], scommon[i + s].fp, sizeof(fingerprint));

				(m[*m_count].arr)[order] = scommon[i + s].size;
				//in ==1, mean it in the scommon
				(m[*m_count].arr)[order + total_num] = 1;
				//cid
				//(m[*m_count].cids)[order] = s1_2[i + s].cid;
			}

			(*m_count)++;
		}

		// the file is not common file.

		i = i + j;
	}

	ff = (struct file_fp *)realloc(ff, *ff_count * sizeof(struct file_fp));
	*fff = ff;

	if (enable_migration == 1)
	{
		m = (struct migrate *)realloc(m, *m_count * sizeof(struct migrate));
		*mm = m;
	}
}

//check migration files in S1 S2
void migration_find(struct migrate *m, int64_t m_count, struct summary *s_c, int64_t s_count)
{
	int64_t i, j, n, begin, k;
	for (i = 0; i < m_count; i++)
	{
		int64_t fid = m[i].fid;
		//get the first in[j]
		for (j = 0; j < m[i].total_num; j++)
		{
			if ((m[i].arr)[j + m[i].total_num] == 1)
				break;
		}
		n = j;

		for (j = 0; j < s_count; j++)
		{
			//find that the location, we will find a lot same fp but not the real same file
			if (comp_code((m[i].fps)[n], s_c[j].fp) == 0 && fid == s_c[j].fid)
			{
				// if(LEVEL >=2)
				// {
				//     char code[41];
				//     hash2code((m[i].fps)[n], code);
				//     code[40] = 0;

				//     char code1[41];
				//     hash2code(s_c[j].fp, code1);
				//     code1[40] = 0;
				//     VERBOSE("   m code=%s; s_c code=%s, begin=%d\n", code, code1, j-s_c[j].order);
				// }
				break;
			}
		}
		begin = j - s_c[j].order;

		for (k = 0; k < m[i].total_num; k++)
		{
			if (s_c[begin + k].fid != fid)
			{
				VERBOSE("ASSERT, first=[%8"PRId64"](%d), location=S(%d), FID NOT EQUAL!\n", fid, n, begin);
			}

			memcpy((m[i].fps)[k], s_c[begin + k].fp, sizeof(fingerprint));
			(m[i].arr)[k] = s_c[begin + k].size;
			//cid
			//(m[i].cids)[k] = s_c[begin + k].cid;
		}
	}
}

void all_refs_find(struct summary *s, int64_t s_count, struct target **r, int64_t *r_count)
{
	int64_t i;

	struct target *rr = (struct target *)malloc(s_count * sizeof(struct target));

	for (i = 0; i < s_count;)
	{
		int64_t j = 1;
		while (i + j < s_count && (comp_code(s[i].fp, s[i + j].fp) == 0))
			j++;

		memcpy(rr[*r_count].fp, s[i].fp, sizeof(fingerprint));
		rr[*r_count].refs = j;
		//cid
		rr[*r_count].cid = s[i].cid;
		(*r_count)++;

		i = i + j;
	}

	rr = (struct target *)realloc(rr, *r_count * sizeof(struct target));
	*r = rr;
}

//-1->no;
int64_t check_in_r(struct target *r, int64_t count, fingerprint fp)
{
	//binarySearch
	int64_t begin = 0, end = count - 1;
	while (begin <= end)
	{
		int64_t mid = (begin + end) / 2;
		int64_t k = comp_code(r[mid].fp, fp);
		if (k == 0)
		{
			return mid;
		}
		else if (k < 0)
			begin = mid + 1;
		else
			end = mid - 1;
	}
	return -1;
}

//-1>no for fingerprint*
int64_t check_in_f(fingerprint *f, int64_t count, fingerprint fp)
{
	//binarySearch
	int64_t begin = 0, end = count - 1;
	while (begin <= end)
	{
		int64_t mid = (begin + end) / 2;
		int64_t k = comp_code(f[mid], fp);
		if (k == 0)
		{
			return mid;
		}
		else if (k < 0)
			begin = mid + 1;
		else
			end = mid - 1;
	}
	return -1;
}

//only for refs>1, refs ==1 just put in the dedup if it appears.
void target_refs_find(struct target *r, int64_t r_count, struct target **t, int64_t *t_count, struct file_fp *ff, int64_t ff_count)
{
	int64_t i, j;

	//mark
	char *list = (char *)calloc(1, r_count * sizeof(char));

	struct target *tt = (struct target *)malloc(r_count * sizeof(struct target));

	for (i = 0; i < ff_count; i++)
	{
		int64_t num = ff[i].num;
		for (j = 0; j < num; j++)
		{
			//r is sorted by fp, ff[i].fp is sorted by it, too
			int64_t mid = check_in_r(r, r_count, (ff[i].fps)[j]);

			if (mid != -1)
			{
				list[mid] = 1;
			}
		}
	}

	if (enable_refs == 0)
	{
		for (i = 0; i < r_count; i++)
		{
			if (list[i] == 1)
			{
				memcpy(tt[*t_count].fp, r[i].fp, sizeof(fingerprint));
				tt[*t_count].refs = r[i].refs;
				tt[*t_count].cid = r[i].cid;
				(*t_count)++;
			}
		}
	}
	else
	{
		for (i = 0; i < r_count; i++)
		{
			if (list[i] == 1 && r[i].refs >1)
			{
				memcpy(tt[*t_count].fp, r[i].fp, sizeof(fingerprint));
				tt[*t_count].refs = r[i].refs;
				tt[*t_count].cid = r[i].cid;
				(*t_count)++;
			}
		}
	}

	//tt is sorted by fp
	tt = (struct target *)realloc(tt, *t_count * sizeof(struct target));
	(*t) = tt;

	free(list);
}

void inter_t1_t2(fingerprint **t1_2, int64_t *t1_2_count, struct target *t1, int64_t t1_count, struct target *t2, int64_t t2_count)
{
	int64_t i = 0, j = 0;
	fingerprint *tt = (fingerprint *)malloc(t1_count * sizeof(fingerprint));

	while (i < t1_count && j < t2_count)
	{
		if (comp_code(t1[i].fp, t2[j].fp) == 0)
		{
			memcpy(tt[*t1_2_count], t1[i].fp, sizeof(fingerprint));
			(*t1_2_count)++;
			i++;
			j++;

			// char code[41];
			// hash2code(tt[*t1_2_count-1], code);
			// code[40] = 0;
			// VERBOSE("T1_2 No.%d = %s\n", *t1_2_count-1, code);
		}
		else if (comp_code(t1[i].fp, t2[j].fp) > 0)
		{
			j++;
		}
		else
		{
			i++;
		}
	}
	tt = (fingerprint *)realloc(tt, *t1_2_count * sizeof(fingerprint));
	*t1_2 = tt;

	if (LEVEL >= 2)
	{
		//show T1_2
		for (i = 0; i < *t1_2_count; i++)
		{
			char code[41];
			hash2code(tt[i], code);
			code[40] = 0;
			VERBOSE("T1,T2 INTERSECTION No.%d = %s\n", i, code);
		}
	}

	if (LEVEL > 1)
		VERBOSE("\nTOTAL T1,T2 INTERSECTION %" PRId64";\n", *t1_2_count);
}

static void pick_fp1(fingerprint *min, fingerprint *fps, int64_t num)
{
	int64_t i;
	//choose the small one

	int64_t k = 0;
	for (i = 0; i < num; i++)
	{
		if (comp_code(fps[i], fps[k]) < 0)
			k = i;
	}

	memcpy(min, fps[k], sizeof(fingerprint));
}

// static int64_t pick_fp(fingerprint **min,fingerprint *fps, int64_t num)
// {
// 	int64_t i,j;
// 	//choose the small one
// 	int64_t size = num > ENABLE_TOPK ? ENABLE_TOPK : num;
// 	*min=(fingerprint*)malloc(size*sizeof(fingerprint));
// 	int64_t *visit=(int64_t*)calloc(1,num*sizeof(int64_t)); //0

// 	int64_t k=0;

// 	int64_t min_now=0;
// 	for(i=0;i<size;i++)
// 	{
// 		for(j=0;j<num;j++)
// 		{
// 			if(visit[j] ==0 && comp_code(fps[i],fps[min_now])<0)
// 				min_now=i;
// 		}
// 		visit[min_now]=1;
// 	}

// 	for(i=0;i<size;i++)
// 	{
// 		for(j=0;j<num;j++)
// 		{
// 			if(visit[j]==1)
// 				memcpy(min[k++],fps[j],sizeof(fingerprint));
// 		}

// 	}

// 	free(visit);
// 	//sort fps
// 	qsort(*min,size,sizeof(fingerprint),comp_fp);

// 	return size;

// }

void top1(struct summary **s1, int64_t *s1_count, struct summary **s2, int64_t *s2_count, \
struct map_recipe **mr1, int64_t *mr1_count, struct map_recipe **mr2, int64_t *mr2_count)
{
	struct top1 *tp1, *tp2;

	tp1 = (struct top1*)malloc(*s1_count*sizeof(struct top1));
	tp2 = (struct top1*)malloc(*s2_count*sizeof(struct top1));
	int64_t tp1_count = 0, tp2_count = 0;

	int64_t i, j;
	int64_t fid_c1 = -1;
	int64_t index_mr1 = 0;
	int64_t fid_c2 = -1;
	int64_t index_mr2 = 0;

	//s1
	for (i = 0; i < *s1_count;)
	{
		if ((*s1)[i].fid != fid_c1)
		{
			index_mr1++;
			fid_c1 = (*s1)[i].fid;
		}

		j = 1;
		while (i + j < *s1_count && (*s1)[i].fid == (*s1)[i + j].fid)
			j++;

		fingerprint *fps = (fingerprint*)malloc(j*sizeof(fingerprint));
		int64_t k;
		for (k = 0; k < j; k++)
			memcpy(fps[k], (*s1)[i + k].fp, sizeof(fingerprint));

		//tp1[tp1_count].fid=(*s1)[i].fid;
		tp1[tp1_count].index_s = i;//mark the index of S1, if it !=0, means the fid can be put in.
		tp1[tp1_count].index_mr = index_mr1;
		//enable
		tp1[tp1_count].enable = 0;
		pick_fp1(&(tp1[tp1_count].fp), fps, j);
		tp1_count++;

		i = i + j;
		free(fps);
		//get all the feature
	}

	//s2
	for (i = 0; i < *s2_count;)
	{
		if ((*s2)[i].fid != fid_c2)
		{
			index_mr2++;
			fid_c2 = (*s2)[i].fid;
		}

		j = 1;
		while (i + j < *s2_count && (*s2)[i].fid == (*s2)[i + j].fid)
			j++;

		fingerprint *fps = (fingerprint*)malloc(j*sizeof(fingerprint));
		int64_t k;
		for (k = 0; k < j; k++)
			memcpy(fps[k], (*s2)[i + k].fp, sizeof(fingerprint));

		//tp2[tp2_count].fid=(*s2)[i].fid;
		tp2[tp2_count].index_s = i;//mark the index of S1, if index_s !=0, then it means the fid can be put in.
		tp2[tp2_count].index_mr = index_mr2;
		//enable
		tp2[tp2_count].enable = 0;
		pick_fp1(&(tp2[tp2_count].fp), fps, j);
		tp2_count++;

		i = i + j;
		free(fps);
		//get all the feature
	}

	tp1 = (struct top1*)realloc(tp1, tp1_count*sizeof(struct top1));
	tp2 = (struct top1*)realloc(tp2, tp2_count*sizeof(struct top1));

	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		for (i = 0; i < tp1_count; i++)
		{
			char code[41];
			hash2code(tp1[i].fp, code);
			code[40] = 0;
			VERBOSE("TP1 MIN fp No.%"PRId64" = %s\n", i, code);
		}

		VERBOSE("\n----------------------------------------------------------------\n");

		for (i = 0; i < tp2_count; i++)
		{
			char code[41];
			hash2code(tp2[i].fp, code);
			code[40] = 0;
			VERBOSE("TP2 MIN fp No.%"PRId64" = %s\n", i, code);
		}

		VERBOSE("\n----------------------------------------------------------------\n");
	}

	if (LEVEL >= 1)
	{
		VERBOSE("TP1 COUNT = %"PRId64", TP2 COUNT = %"PRId64";\n", tp1_count, tp2_count);
	}

	//compare TP1, TP2
	//sort by top fps[0];
	qsort(tp1, tp1_count, sizeof(struct top1), comp_tp1);
	qsort(tp2, tp2_count, sizeof(struct top1), comp_tp1);

	//deal with whether the file is in or not
	//as for tp1

	struct summary *new_s1 = (struct summary *)malloc(*s1_count*sizeof(struct summary));
	struct summary *new_s2 = (struct summary *)malloc(*s2_count*sizeof(struct summary));
	int64_t new_s1_count = 0, new_s2_count = 0;

	struct map_recipe *new_mr1 = (struct map_recipe *)malloc(*mr1_count*sizeof(struct map_recipe));
	struct map_recipe *new_mr2 = (struct map_recipe *)malloc(*mr2_count*sizeof(struct map_recipe));
	int64_t new_mr1_count = 0, new_mr2_count = 0;

	int64_t a = 0, b = 0;
	while (a < tp1_count && b < tp2_count)
	{
		//match, enable
		if (comp_code(tp1[a].fp, tp2[b].fp) != 0)
		{
			tp1[a].enable = 1; //enable it
			tp2[b].enable = 1;
			a++;
			b++;
			// char code[41];
			// hash2code(s1_2[*s1_2_count-1].fp, code);
			// code[40] = 0;
			// VERBOSE("No.%" PRId64 " [%8" PRId64 "][%8" PRId64 "] = %s\n", *s1_2_count-1, s1[a-1].fid, s2[b-1].fid, code);
		}
		else if (comp_code(tp1[a].fp, tp2[b].fp) > 0)
		{
			b++;
		}
		else
		{
			a++;
		}
	}

	//put needed files' chunks into new s1
	for (i = 0; i < tp1_count; i++)
	{
		if (tp1[i].enable == 1)
		{
			//s1
			int64_t index = tp1[i].index_s;
			new_s1[new_s1_count++] = (*s1)[index];

			j = 1;
			while (index - j >= 0 && (*s1)[index].fid == (*s1)[index - j].fid)
			{
				new_s1[new_s1_count++] = (*s1)[index - j];
				j++;
			}

			j = 1;
			while (index + j < *s1_count && (*s1)[index].fid == (*s1)[index + j].fid)
			{
				new_s1[new_s1_count++] = (*s1)[index + j];
				j++;
			}

			//mr1
			new_mr1[new_mr1_count++] = (*mr1)[tp1[i].index_mr];
		}
	}

	//put needed files' chunks into new s2
	for (i = 0; i < tp2_count; i++)
	{
		if (tp2[i].enable == 1)
		{
			//s2
			int64_t index = tp2[i].index_s;
			new_s2[new_s2_count++] = (*s2)[index];

			j = 1;
			while (index - j >= 0 && (*s2)[index].fid == (*s2)[index - j].fid)
			{
				new_s2[new_s2_count++] = (*s2)[index - j];
				j++;
			}

			j = 1;
			while (index + j < *s2_count && (*s2)[index].fid == (*s2)[index + j].fid)
			{
				new_s2[new_s2_count++] = (*s2)[index + j];
				j++;
			}

			//mr2
			new_mr2[new_mr2_count++] = (*mr2)[tp2[i].index_mr];
		}
	}

	free(*s1);
	free(*s2);
	free(*mr1);
	free(*mr2);

	new_s1 = (struct summary*)realloc(new_s1, new_s1_count*sizeof(struct summary));
	new_s2 = (struct summary*)realloc(new_s2, new_s2_count*sizeof(struct summary));

	new_mr1 = (struct map_recipe*)realloc(new_mr1, new_mr1_count*sizeof(struct map_recipe));
	new_mr2 = (struct map_recipe*)realloc(new_mr2, new_mr2_count*sizeof(struct map_recipe));

	*s1 = new_s1;
	*s2 = new_s2;
	*s1_count = new_s1_count;
	*s2_count = new_s2_count;

	*mr1 = new_mr1;
	*mr2 = new_mr2;
	*mr1_count = new_mr1_count;
	*mr2_count = new_mr2_count;
}

//statistic avg groups of one file, MAXcontainerid / N

int64_t total_groups(struct summary *s, int64_t s_count, int64_t max_cid)
{
	int64_t i, j;
	int64_t total = 0;

	int64_t seg = max_cid / GROUPS;
	int64_t file = 0;

	for (i = 0; i < s_count;)
	{
		j = 1;
		while (i + j < s_count && s[i].fid == s[i + j].fid)
			j++;
		file++;
		int visit[GROUPS] = { 0 };
		int a;
		for (a = 0; a < j; a++)
		{
			int l = s[i + a].cid / seg;
			//int l = s[i+a].cid % GROUPS;
			if (visit[l] == 0)
			{
				total++;
				visit[l] = 1;
			}
			//printf("seg=%"PRId64", cid=%ld, file=%ld, l=%d, total=%ld\n",seg, s[i+a].cid, file, l,total);
		}

		i = i + j;
	}

	//printf("%"PRId64"\n",file);
	return total;
}