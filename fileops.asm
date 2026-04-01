# =============================================================
# Makefile — builds the native shared library
# =============================================================
#
# WHAT IS A MAKEFILE?
# A Makefile automates the build process.
# Instead of typing long commands, you just run: make
#
# HOW TO USE:
# Build: make
# Clean: make clean
# Test: make test
#
# WHAT GETS BUILT:
# fileops.asm → (nasm) → fileops.o (assembled object file)
# wrapper.c → (gcc) → wrapper.o (compiled object file)
# fileops.o + wrapper.o → (gcc) → libfileops.so (shared library)
#
# Python will load libfileops.so using ctypes.

# Tools
ASM = nasm
CC = gcc

# Flags
ASMFLAGS = -f elf64 # output format: 64-bit ELF (Linux)
CFLAGS = -Wall -O2 -fPIC # -fPIC = Position Independent Code (required for .so)
LDFLAGS = -shared # create a shared library (.so), not an executable

# Output
TARGET = libfileops.so

# Source files
ASM_SRC = fileops.asm
C_SRC = wrapper.c

# Object files
ASM_OBJ = fileops.o
C_OBJ = wrapper.o

# Default target: build everything
all: $(TARGET)
	@echo ""
	@echo "✅ Built $(TARGET) successfully!"
	@echo " Load it in Python with: ctypes.CDLL('./native/libfileops.so')"

# Step 1: Assemble .asm → .o
$(ASM_OBJ): $(ASM_SRC)
	@echo "🔧 Assembling $(ASM_SRC) → $(ASM_OBJ)"
	$(ASM) $(ASMFLAGS) $(ASM_SRC) -o $(ASM_OBJ)

# Step 2: Compile .c → .o
$(C_OBJ): $(C_SRC)
	@echo "🔧 Compiling $(C_SRC) → $(C_OBJ)"
	$(CC) $(CFLAGS) -c $(C_SRC) -o $(C_OBJ)

# Step 3: Link both .o files → .so shared library
$(TARGET): $(ASM_OBJ) $(C_OBJ)
	@echo "🔗 Linking → $(TARGET)"
	$(CC) $(LDFLAGS) $(ASM_OBJ) $(C_OBJ) -o $(TARGET)

# Remove all build artifacts
clean:
	@echo "🧹 Cleaning build artifacts"
	rm -f $(ASM_OBJ) $(C_OBJ) $(TARGET)

# Quick test: compile and run a small C test program
test: $(TARGET)
	@echo "🧪 Running C test..."
	$(CC) -o test_native test_native.c -L. -lfileops -Wl,-rpath,.
	./test_native

.PHONY: all clean test
