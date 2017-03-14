d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)nopaxos-test.cc

$(d)nopaxos-test: $(o)nopaxos-test.o \
                  $(OBJS-nopaxos-replica) $(OBJS-nopaxos-client) \
                  $(LIB-simtransport) \
                  $(GTEST_MAIN)

TEST_BINS += $(d)nopaxos-test
