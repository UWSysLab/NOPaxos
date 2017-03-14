d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc)

PROTOS += $(addprefix $(d), \
	    unreplicated-proto.proto)

OBJS-unreplicated-client := $(o)client.o $(o)unreplicated-proto.o \
               $(OBJS-client) $(LIB-message) \
               $(LIB-configuration)

OBJS-unreplicated-replica := $(o)replica.o $(o)unreplicated-proto.o \
               $(OBJS-replica) $(LIB-message) \
               $(LIB-configuration)

include $(d)tests/Rules.mk

