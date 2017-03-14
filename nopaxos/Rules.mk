d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc)

PROTOS += $(addprefix $(d), \
	  nopaxos-proto.proto)

OBJS-nopaxos-client := $(o)client.o $(o)nopaxos-proto.o \
		       $(OBJS-client) $(LIB-message) \
		       $(LIB-configuration)

OBJS-nopaxos-replica := $(o)replica.o $(o)nopaxos-proto.o \
		        $(OBJS-replica) $(LIB-message) \
		        $(LIB-configuration)

include $(d)tests/Rules.mk
