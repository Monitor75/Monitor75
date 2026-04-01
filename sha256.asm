; =============================================================================
; sha256.asm — x86-64 Assembly SHA-256 Implementation
; -----------------------------------------------------------------------------
; Architecture : x86-64 (AMD64)
; ABI          : System V AMD64 (Linux)
; Assembler    : NASM 2.14+
; -----------------------------------------------------------------------------
; Signature:
;   void sha256_asm(const unsigned char *input, size_t length,
;                   unsigned char *output)
;
;   rdi = input  — pointer to padded message bytes (multiple of 64)
;   rsi = length — padded byte length (multiple of 64)
;   rdx = output — pointer to 32-byte output buffer
;
; All data references use RIP-relative addressing (default rel) so this object
; is fully position-independent and links correctly in both PIE and non-PIE
; x86-64 ELF binaries.
;
; Stack frame (rbp = base, allocated with sub rsp,328):
;   [rbp+  0 .. 255]  W[0..63]  — 256-byte message schedule
;   [rbp+256 .. 287]  pre-round hash snapshot (8 × dword)
;   [rbp+288]         saved rdi  (input block pointer)
;   [rbp+296]         saved rsi  (remaining byte count)
;   [rbp+304]         saved rdx  (output pointer)
; =============================================================================

default rel                     ; all label refs → RIP-relative (safe in PIE)

section .rodata
align 64

; SHA-256 round constants K[0..63] — FIPS 180-4 §4.2.2
K:
    dd 0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5
    dd 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5
    dd 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3
    dd 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174
    dd 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc
    dd 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da
    dd 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7
    dd 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967
    dd 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13
    dd 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85
    dd 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3
    dd 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070
    dd 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5
    dd 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3
    dd 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208
    dd 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2

; SHA-256 initial hash values H[0..7] — FIPS 180-4 §5.3.3
H_INIT:
    dd 0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a
    dd 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19

; =============================================================================
section .text
global sha256_asm

; -----------------------------------------------------------------------------
; ROUND i
;   Register map:  r12d=a  r13d=b  r14d=c  r15d=d
;                  r8d=e   r9d=f   r10d=g  r11d=h
;   Clobbers: eax, ebx, ecx, edx
;   r_kbase must point to K[] (loaded before the round loop, held in a
;   callee-saved register so it survives all 64 rounds without memory refs).
; -----------------------------------------------------------------------------
%macro ROUND 1
    ; ── T1 = h + Σ1(e) + Ch(e,f,g) + K[i] + W[i] ──────────────────────────
    mov  eax, r8d
    ror  eax, 6
    mov  ebx, r8d
    ror  ebx, 11
    xor  eax, ebx
    mov  ebx, r8d
    ror  ebx, 25
    xor  eax, ebx           ; eax = Σ1(e)

    add  eax, r11d          ; + h

    mov  ebx, r8d
    and  ebx, r9d           ; e AND f
    mov  ecx, r8d
    not  ecx
    and  ecx, r10d          ; NOT(e) AND g
    xor  ebx, ecx
    add  eax, ebx           ; + Ch(e,f,g)

    add  eax, [rbp + %1*4]  ; + W[i]   (rbp = &W[0], stable throughout)
    add  eax, [r_kbase + %1*4] ; + K[i] (r_kbase = &K[0], RIP-relative loaded once)
    ; eax = T1

    ; ── T2 = Σ0(a) + Maj(a,b,c) ────────────────────────────────────────────
    mov  ebx, r12d
    ror  ebx, 2
    mov  ecx, r12d
    ror  ecx, 13
    xor  ebx, ecx
    mov  ecx, r12d
    ror  ecx, 22
    xor  ebx, ecx           ; ebx = Σ0(a)

    mov  ecx, r12d
    and  ecx, r13d          ; a AND b
    mov  edx, r12d
    and  edx, r14d          ; a AND c
    xor  ecx, edx
    mov  edx, r13d
    and  edx, r14d          ; b AND c
    xor  ecx, edx
    add  ebx, ecx           ; ebx = T2

    ; ── Rotate working variables ─────────────────────────────────────────────
    mov  r11d, r10d         ; h = g
    mov  r10d, r9d          ; g = f
    mov  r9d,  r8d          ; f = e
    mov  r8d,  r15d
    add  r8d,  eax          ; e = d + T1
    mov  r15d, r14d         ; d = c
    mov  r14d, r13d         ; c = b
    mov  r13d, r12d         ; b = a
    mov  r12d, eax
    add  r12d, ebx          ; a = T1 + T2
%endmacro

; Alias: r_kbase = register that holds the pointer to K[]
; We repurpose rbx as r_kbase across the round loop (rbx is callee-saved,
; and the round macro only uses ebx as a scratch register, restoring it each
; time through the Add/Maj steps — so we reload rbx from the shadow before
; using it as r_kbase).
;
; Actually, simpler and safer: use a scratch slot in our frame.
; We give r_kbase its own shadow at [rbp+312] (within our 328-byte alloc).
%define r_kbase [rbp+312]

; =============================================================================
sha256_asm:
    ; ── Prologue ──────────────────────────────────────────────────────────────
    push rbp
    push rbx
    push r12
    push r13
    push r14
    push r15

    ; 6 pushes × 8 = 48 bytes; sub 328 → total frame 376, 376 % 16 = 8 before
    ; the ret-address push, so after call rsp was already -8 → total = 384 % 16 = 0 ✓
    sub  rsp, 328
    mov  rbp, rsp           ; rbp = &W[0] — never changes

    ; Cache K[] pointer via RIP-relative lea (works in PIE and non-PIE)
    lea  rax, [K]
    mov  [rbp + 312], rax   ; r_kbase shadow slot

    ; Save caller registers
    mov  [rbp + 288], rdi
    mov  [rbp + 296], rsi
    mov  [rbp + 304], rdx

    ; ── Load initial hash values (RIP-relative) ───────────────────────────────
    lea  rax, [H_INIT]
    mov  r12d, [rax +  0]   ; a
    mov  r13d, [rax +  4]   ; b
    mov  r14d, [rax +  8]   ; c
    mov  r15d, [rax + 12]   ; d
    mov  r8d,  [rax + 16]   ; e
    mov  r9d,  [rax + 20]   ; f
    mov  r10d, [rax + 24]   ; g
    mov  r11d, [rax + 28]   ; h

    ; =========================================================================
    ; Block loop — process one 64-byte (512-bit) block per iteration
    ; =========================================================================
.block_loop:
    cmp  rsi, 64
    jb   .finalize

    ; ── Step 1: Load W[0..15] from big-endian input ───────────────────────────
    xor  rcx, rcx
.load_words:
    mov  eax, [rdi + rcx*4]
    bswap eax
    mov  [rbp + rcx*4], eax
    inc  rcx
    cmp  rcx, 16
    jb   .load_words

    ; ── Step 2: Extend W[16..63] ──────────────────────────────────────────────
    ; σ0(x) = ROTR7(x)  XOR ROTR18(x) XOR SHR3(x)
    ; σ1(x) = ROTR17(x) XOR ROTR19(x) XOR SHR10(x)
    ; W[i]  = σ1(W[i-2]) + W[i-7] + σ0(W[i-15]) + W[i-16]
    mov  rcx, 16
.extend:
    ; σ0(W[i-15])
    mov  eax, [rbp + rcx*4 - 60]
    mov  ebx, eax
    ror  eax, 7
    ror  ebx, 18
    xor  eax, ebx
    mov  ebx, [rbp + rcx*4 - 60]
    shr  ebx, 3
    xor  eax, ebx           ; eax = σ0

    ; σ1(W[i-2])
    mov  edx, [rbp + rcx*4 - 8]
    mov  ebx, edx
    ror  edx, 17
    ror  ebx, 19
    xor  edx, ebx
    mov  ebx, [rbp + rcx*4 - 8]
    shr  ebx, 10
    xor  edx, ebx           ; edx = σ1

    add  eax, [rbp + rcx*4 - 64]  ; + W[i-16]
    add  eax, [rbp + rcx*4 - 28]  ; + W[i-7]
    add  eax, edx
    mov  [rbp + rcx*4], eax

    inc  rcx
    cmp  rcx, 64
    jb   .extend

    ; ── Step 3: Snapshot hash state entering this block ───────────────────────
    mov  [rbp + 256 +  0], r12d
    mov  [rbp + 256 +  4], r13d
    mov  [rbp + 256 +  8], r14d
    mov  [rbp + 256 + 12], r15d
    mov  [rbp + 256 + 16], r8d
    mov  [rbp + 256 + 20], r9d
    mov  [rbp + 256 + 24], r10d
    mov  [rbp + 256 + 28], r11d

    ; Save rdi/rsi/rdx (ROUND macro clobbers edx via Maj computation)
    mov  [rbp + 288], rdi
    mov  [rbp + 296], rsi
    mov  [rbp + 304], rdx

    ; ── Step 4: 64 rounds ─────────────────────────────────────────────────────
    ROUND  0
    ROUND  1
    ROUND  2
    ROUND  3
    ROUND  4
    ROUND  5
    ROUND  6
    ROUND  7
    ROUND  8
    ROUND  9
    ROUND 10
    ROUND 11
    ROUND 12
    ROUND 13
    ROUND 14
    ROUND 15
    ROUND 16
    ROUND 17
    ROUND 18
    ROUND 19
    ROUND 20
    ROUND 21
    ROUND 22
    ROUND 23
    ROUND 24
    ROUND 25
    ROUND 26
    ROUND 27
    ROUND 28
    ROUND 29
    ROUND 30
    ROUND 31
    ROUND 32
    ROUND 33
    ROUND 34
    ROUND 35
    ROUND 36
    ROUND 37
    ROUND 38
    ROUND 39
    ROUND 40
    ROUND 41
    ROUND 42
    ROUND 43
    ROUND 44
    ROUND 45
    ROUND 46
    ROUND 47
    ROUND 48
    ROUND 49
    ROUND 50
    ROUND 51
    ROUND 52
    ROUND 53
    ROUND 54
    ROUND 55
    ROUND 56
    ROUND 57
    ROUND 58
    ROUND 59
    ROUND 60
    ROUND 61
    ROUND 62
    ROUND 63

    ; ── Step 5: Restore pointers, add snapshot → running hash ─────────────────
    mov  rdi, [rbp + 288]
    mov  rsi, [rbp + 296]
    mov  rdx, [rbp + 304]

    add  r12d, [rbp + 256 +  0]
    add  r13d, [rbp + 256 +  4]
    add  r14d, [rbp + 256 +  8]
    add  r15d, [rbp + 256 + 12]
    add  r8d,  [rbp + 256 + 16]
    add  r9d,  [rbp + 256 + 20]
    add  r10d, [rbp + 256 + 24]
    add  r11d, [rbp + 256 + 28]

    ; Advance to next block
    add  rdi, 64
    sub  rsi, 64
    mov  [rbp + 288], rdi
    mov  [rbp + 296], rsi
    jmp  .block_loop

    ; =========================================================================
    ; Finalize — write 256-bit digest in big-endian order
    ; =========================================================================
.finalize:
    bswap r12d
    mov   [rdx +  0], r12d
    bswap r13d
    mov   [rdx +  4], r13d
    bswap r14d
    mov   [rdx +  8], r14d
    bswap r15d
    mov   [rdx + 12], r15d
    bswap r8d
    mov   [rdx + 16], r8d
    bswap r9d
    mov   [rdx + 20], r9d
    bswap r10d
    mov   [rdx + 24], r10d
    bswap r11d
    mov   [rdx + 28], r11d

    ; ── Epilogue ──────────────────────────────────────────────────────────────
    add  rsp, 328
    pop  r15
    pop  r14
    pop  r13
    pop  r12
    pop  rbx
    pop  rbp
    ret

section .note.GNU-stack noalloc noexec nowrite progbits
; =============================================================================
; End of sha256.asm
; =============================================================================
