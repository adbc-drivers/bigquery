# Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.

ifeq ($(shell go env GOOS), linux)
	RM=rm -f
	PREFIX=lib
	SUFFIX=so
else ifeq ($(shell go env GOOS), darwin)
	RM=rm -f
	PREFIX=lib
	SUFFIX=dylib
else ifeq ($(shell go env GOOS), windows)
	RM=del
	PREFIX=
	SUFFIX=dll
else
	$(error Unsupported OS)
endif

DRIVERS := \
	libadbc_driver_bigquery.$(SUFFIX)

.PHONY: all clean
all: $(DRIVERS)

clean:
	$(RM) $(DRIVERS)

libadbc_driver_%.$(SUFFIX): % $(wildcard %/*.go %/pkg/*.go %/pkg/*.c %/pkg/%.h)
	go build -C ./$</pkg -o ../../$@ -buildmode=c-shared -tags driverlib -ldflags "-s -w" .
	-$(RM) ./$(basename $@).h
	chmod 755 $@
