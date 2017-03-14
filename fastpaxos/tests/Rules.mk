d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)fastpaxos-test.cc

$(d)fastpaxos-test: $(o)fastpaxos-test.o \
	$(OBJS-fastpaxos-replica) $(OBJS-fastpaxos-client) \
	$(LIB-simtransport) \
	$(GTEST_MAIN)

TEST_BINS += $(d)fastpaxos-test
