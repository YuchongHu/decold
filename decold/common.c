#include "common.h"

void hash2code(unsigned char hash[20], char code[40])
{
	int i, j, b;
	unsigned char a, c;
	i = 0;
	for (i = 0; i < 20; i++)
	{
		a = hash[i];
		for (j = 0; j < 2; j++)
		{
			b = a / 16;
			switch (b)
			{
			case 10:
				c = 'A';
				break;
			case 11:
				c = 'B';
				break;
			case 12:
				c = 'C';
				break;
			case 13:
				c = 'D';
				break;
			case 14:
				c = 'E';
				break;
			case 15:
				c = 'F';
				break;
			default:
				c = b + 48;
				break;
			}
			code[2 * i + j] = c;
			a = a << 4;
		}
	}
}

void decold_log(const char *fmt, ...)
{
	va_list ap;
	char msg[1024];

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stdout, "%s", msg);
}

int comp_code(unsigned char hash1[20], unsigned char hash2[20])
{
	int i = 0;
	while (i<20)
	{
		if (hash1[i]>hash2[i])
			return 1;
		else if (hash1[i] < hash2[i])
			return -1;
		else
			i++;
	}

	return 0;
}

gboolean g_fingerprint_equal(const void *fp1, const void *fp2)
{
	return !memcmp((fingerprint*)fp1, (fingerprint*)fp2, sizeof(fingerprint));
}

int32_t unserial_int32(uint8_t * * const ptr)
{
	int32_t vo;

	memcpy(&vo, *ptr, sizeof vo);
	*ptr += sizeof vo;
	return ntohl(vo);
}

int64_t unserial_int64(uint8_t * * const ptr)
{
	int64_t v;

	if (htonl(1) == 1L) {
		memcpy(&v, *ptr, sizeof(int64_t));
	}
	else {
		int i;
		uint8_t rv[sizeof(int64_t)];
		uint8_t *pv = (uint8_t *)&v;

		memcpy(&v, *ptr, sizeof(int64_t));
		for (i = 0; i < 8; i++) {
			rv[i] = pv[7 - i];
		}
		memcpy(&v, &rv, sizeof(int64_t));
	}
	*ptr += sizeof(int64_t);
	return v;
}

void print_unsigned(unsigned char *u, int64_t len)
{
	int64_t i;
	for (i = 0; i < len; i++)
		printf("%hhu", u[i]);
	printf("\n");
}