#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>

extern int enable_migration;
extern int enable_refs;
extern int enable_topk;
extern long int big_file;
extern float migration_threshold;
extern char db_path[];
extern int db_is_exist;

void parse_line(char *line) {
	char *token = strtok(line, " ");
	if (0 == strcmp(token, "enable_migration")) {
		if (NULL == (token = strtok(NULL, " "))) {
			printf("config:enable_migration no value");
			return;
		}
		if (strcmp(token, "yes"))
			enable_migration = 1;
		else
			enable_migration = 0;
	}
	else if (0 == strcmp(token, "enable_refs")) {
		if (NULL == (token = strtok(NULL, " "))) {
			printf("config:enable_refs no value");
			return;
		}
		if (strcmp(token, "yes"))
			enable_refs = 1;
		else
			enable_refs = 0;
	}
	else if (0 == strcmp(token, "enable_topk")) {
		if (NULL == (token = strtok(NULL, " "))) {
			printf("config:enable_topk no value");
			return;
		}
		if (strcmp(token, "yes"))
			enable_refs = 1;
		else
			enable_refs = 0;
	}
	else if (0 == strcmp(token, "big_file")) {
		if (NULL == (token = strtok(NULL, " "))) {
			printf("config:big_file no value");
			return;
		}
		sscanf(token, "%ld", &big_file);
	}
	else if (0 == strcmp(token, "migration_threshold")) {
		if (NULL == (token = strtok(NULL, " "))) {
			printf("config:migration_threshold no value");
			return;
		}
		sscanf(token, "%f", &migration_threshold);
	}
	else if (0 == strcmp(token, "db_path")) {
		if (NULL == (token = strtok(NULL, " "))) {
			printf("config:db_path no value");
			return;
		}
		sscanf(token, "%s", db_path);
	}
	else if (0 == strcmp(token, "db_is_exist")) {
		if (NULL == (token = strtok(NULL, " "))) {
			printf("config:db_is_exist no value");
			return;
		}
		sscanf(token, "%d", &db_is_exist);
	}
}

void load_config() {
	char buf[1024] = { 0 };
	FILE *fp;

	if ((fp = fopen("decold.config", "r")) == 0) {
		printf("open decold.config fail!\n");
		return;
	}

	while (fgets(buf, 1024, fp) != NULL) {
		parse_line(buf);
	}

	fclose(fp);
}