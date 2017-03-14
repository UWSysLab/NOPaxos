d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)spec-test.cc $(d)merge-test.cc
PROTOS += $(d)merge-test-case.proto

$(d)spec-test: $(o)spec-test.o \
	$(OBJS-spec-replica) $(OBJS-spec-client) \
	$(LIB-simtransport) \
	$(GTEST_MAIN)

$(d)merge-test: $(o)merge-test.o $(o)merge-test-case.o \
	$(OBJS-spec-replica) $(OBJS-spec-client) \
	$(LIB-simtransport) \
	$(GTEST_MAIN)

TEST_BINS += $(d)merge-test $(d)spec-test 
