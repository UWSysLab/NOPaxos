d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)unreplicated-test.cc

$(d)unreplicated-test: $(o)unreplicated-test.o \
	$(OBJS-unreplicated-replica) $(OBJS-unreplicated-client) \
        $(LIB-simtransport) \
        $(GTEST_MAIN)

TEST_BINS += $(d)unreplicated-test
