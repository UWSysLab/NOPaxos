d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	sequencer.cc)

$(d)sequencer: $(o)sequencer.o $(LIB-message)

BINS += $(d)sequencer
