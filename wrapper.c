/*
 * test_native.c — standalone C test for the native library
 * =========================================================
 *
 * This is useful to verify your assembly + C code works
 * BEFORE trying to connect it to Python.
 *
 * Build & run:
 * cd native
 * make test
 *
 * Or manually:
 * gcc -o test_native test_native.c -L. -lfileops -Wl,-rpath,.
 * ./test_native
 */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

/* Mirror the struct from wrapper.c */
typedef struct {
    uint32_t adler32;
    uint64_t file_size;
    uint64_t null_bytes;
    uint64_t newline_bytes;
    uint64_t printable_bytes;
    uint8_t min_byte;
    uint8_t max_byte;
    uint8_t is_text;
    uint8_t _pad[1];
} FileStats;

/* Declare functions from the library */
extern uint32_t adler32_clean(const uint8_t *data, size_t length);
extern uint64_t count_bytes(const uint8_t *data, size_t length, uint8_t target);
extern uint8_t get_min_byte(const uint8_t *data, size_t length);
extern uint8_t get_max_byte(const uint8_t *data, size_t length);

/* ANSI color codes for output */
#define GREEN "\033[32m"
#define RED "\033[31m"
#define RESET "\033[0m"

int pass = 0, fail = 0;

void check(const char *test_name, int condition) {
    if (condition) {
        printf(GREEN " ✅ PASS" RESET " — %s\n", test_name);
        pass++;
    } else {
        printf(RED " ❌ FAIL" RESET " — %s\n", test_name);
        fail++;
    }
}

int main() {
    printf("\n=== Native Library Tests ===\n\n");

    /* -------------------------------------------------------
     * Test 1: Adler-32 known values
     * The Adler-32 of "Wikipedia" is 0x11E60398 (from the spec)
     * ------------------------------------------------------- */
    printf("[ adler32_clean ]\n");

    const uint8_t wiki[] = "Wikipedia";
    uint32_t result = adler32_clean(wiki, strlen((char*)wiki));
    printf(" adler32('Wikipedia') = 0x%08X (expected 0x11E60398)\n", result);
    check("adler32('Wikipedia') == 0x11E60398", result == 0x11E60398);

    /* Empty input should return 1 (A=1, B=0) */
    uint32_t empty_result = adler32_clean((uint8_t*)"", 0);
    check("adler32('') == 1", empty_result == 1);

    /* Single byte 'A' (0x41): A=1+65=66, B=0+66=66 → 0x00420042 */
    uint32_t single = adler32_clean((uint8_t*)"A", 1);
    printf(" adler32('A') = 0x%08X (expected 0x00420042)\n", single);
    check("adler32('A') == 0x00420042", single == 0x00420042);

    /* -------------------------------------------------------
     * Test 2: count_bytes
     * ------------------------------------------------------- */
    printf("\n[ count_bytes ]\n");

    const uint8_t data1[] = "hello world";
    uint64_t l_count = count_bytes(data1, sizeof(data1)-1, 'l');
    printf(" count 'l' in 'hello world' = %llu (expected 3)\n", l_count);
    check("count 'l' in 'hello world' == 3", l_count == 3);

    uint64_t x_count = count_bytes(data1, sizeof(data1)-1, 'x');
    check("count 'x' in 'hello world' == 0", x_count == 0);

    const uint8_t zeros[] = {0, 0, 0, 1, 2, 0};
    uint64_t zero_count = count_bytes(zeros, 6, 0);
    check("count 0x00 in {0,0,0,1,2,0} == 4", zero_count == 4);

    /* -------------------------------------------------------
     * Test 3: get_min_byte / get_max_byte
     * ------------------------------------------------------- */
    printf("\n[ get_min_byte / get_max_byte ]\n");

    const uint8_t range[] = {5, 3, 8, 1, 9, 2, 7};
    uint8_t mn = get_min_byte(range, 7);
    uint8_t mx = get_max_byte(range, 7);
    printf(" min of {5,3,8,1,9,2,7} = %u (expected 1)\n", mn);
    printf(" max of {5,3,8,1,9,2,7} = %u (expected 9)\n", mx);
    check("min == 1", mn == 1);
    check("max == 9", mx == 9);

    const uint8_t single_b[] = {42};
    check("min of {42} == 42", get_min_byte(single_b, 1) == 42);
    check("max of {42} == 42", get_max_byte(single_b, 1) == 42);

    /* -------------------------------------------------------
     * Results
     * ------------------------------------------------------- */
    printf("\n=== Results: %d passed, %d failed ===\n\n", pass, fail);
    return (fail > 0) ? 1 : 0;
}
