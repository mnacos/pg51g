# File name: Makefile
SERVER_INCLUDES += -I $(shell pg_config --includedir)
SERVER_INCLUDES += -I $(shell pg_config --includedir-server)
CFLAGS += -O3 $(SERVER_INCLUDES)
.SUFFIXES:      .so
.c.so:
	$(CC) $(CFLAGS) -fpic -c $<
	$(CC) $(CFLAGS) -shared -o $@ $(basename $<).o

