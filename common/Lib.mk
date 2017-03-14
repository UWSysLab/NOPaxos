d := $(dir $(lastword $(MAKEFILE_LIST)))

PROTOS += $(addprefix $(d), \
		request.proto)

LIB-request := $(o)request.o
