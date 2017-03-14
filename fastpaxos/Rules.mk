d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc)

PROTOS += $(addprefix $(d), \
	    fastpaxos-proto.proto)

OBJS-fastpaxos-client := $(o)client.o $(o)fastpaxos-proto.o \
                   $(OBJS-client) $(LIB-message) \
                   $(LIB-configuration)

OBJS-fastpaxos-replica := $(o)replica.o $(o)fastpaxos-proto.o \
                   $(OBJS-replica) $(LIB-message) \
                   $(LIB-configuration)

#include $(d)tests/Rules.mk
