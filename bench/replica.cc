// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replica/main.cc:
 *   test replica application; "null RPC" equivalent
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "lib/configuration.h"
#include "common/replica.h"
#include "lib/udptransport.h"
#include "fastpaxos/replica.h"
#include "spec/replica.h"
#include "unreplicated/replica.h"
#include "vr/replica.h"
#include "nopaxos/replica.h"

#include <unistd.h>
#include <stdlib.h>
#include <vector>
#include <fstream>
#include <iostream>

#include <sched.h>
#include <thread>

static void
Usage(const char *progName)
{
        fprintf(stderr, "usage: %s -c conf-file [-R] -i replica-index -m unreplicated|vr|fastpaxos|spec|nopaxos [-b batch-size] [-d packet-drop-rate] [-r packet-reorder-rate] [-q dscp]\n",
                progName);
        exit(1);
}


int
main(int argc, char **argv)
{
    int index = -1;
    const char *configPath = NULL;
    double dropRate = 0.0;
    double reorderRate = 0.0;
    int dscp = 0;
    int batchSize = 1;
    bool recover = false;

    specpaxos::AppReplica *nullApp = new specpaxos::AppReplica();

    enum
    {
        PROTO_UNKNOWN,
        PROTO_UNREPLICATED,
        PROTO_VR,
        PROTO_FASTPAXOS,
        PROTO_SPEC,
	PROTO_NOPAXOS
    } proto = PROTO_UNKNOWN;

    // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "b:c:d:i:m:q:r:R:tw:")) != -1) {
        switch (opt) {
        case 'b':
        {
            char *strtolPtr;
            batchSize = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0')
                || (batchSize < 1))
            {
                fprintf(stderr,
                        "option -b requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'c':
            configPath = optarg;
            break;

        case 'd':
        {
            char *strtodPtr;
            dropRate = strtod(optarg, &strtodPtr);
            if ((*optarg == '\0') || (*strtodPtr != '\0') ||
                ((dropRate < 0) || (dropRate >= 1))) {
                fprintf(stderr,
                        "option -d requires a numeric arg between 0 and 1\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'i':
        {
            char *strtolPtr;
            index = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (index < 0))
            {
                fprintf(stderr,
                        "option -i requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'm':
            if (strcasecmp(optarg, "unreplicated") == 0) {
                proto = PROTO_UNREPLICATED;
            } else if (strcasecmp(optarg, "vr") == 0) {
                proto = PROTO_VR;
            } else if (strcasecmp(optarg, "fastpaxos") == 0) {
                proto = PROTO_FASTPAXOS;
            } else if (strcasecmp(optarg, "spec") == 0) {
                proto = PROTO_SPEC;
	    } else if (strcasecmp(optarg, "nopaxos") == 0) {
		proto = PROTO_NOPAXOS;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                Usage(argv[0]);
            }
            break;

        case 'q':
        {
            char *strtolPtr;
            dscp = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (dscp < 0))
            {
                fprintf(stderr,
                        "option -q requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'r':
        {
            char *strtodPtr;
            reorderRate = strtod(optarg, &strtodPtr);
            if ((*optarg == '\0') || (*strtodPtr != '\0') ||
                ((reorderRate < 0) || (reorderRate >= 1))) {
                fprintf(stderr,
                        "option -r requires a numeric arg between 0 and 1\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'R':
            recover = true;
            break;

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            Usage(argv[0]);
            break;
        }
    }

    if (!configPath) {
        fprintf(stderr, "option -c is required\n");
        Usage(argv[0]);
    }
    if (index == -1) {
        fprintf(stderr, "option -i is required\n");
        Usage(argv[0]);
    }
    if (proto == PROTO_UNKNOWN) {
        fprintf(stderr, "option -m is required\n");
        Usage(argv[0]);
    }
    if ((proto != PROTO_VR) && (batchSize != 1)) {
        Warning("Batching enabled, but has no effect on non-VR protocols");
    }

    // Load configuration
    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                configPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration config(configStream);

    if (index >= config.n) {
        fprintf(stderr, "replica index %d is out of bounds; "
                "only %d replicas defined\n", index, config.n);
        Usage(argv[0]);
    }

    UDPTransport transport(dropRate, reorderRate, dscp, nullptr);

    specpaxos::Replica *replica;
    switch (proto) {
    case PROTO_UNREPLICATED:
        replica =
            new specpaxos::unreplicated::UnreplicatedReplica(config,
                                                             index,
                                                             !recover,
                                                             &transport,
                                                             nullApp);
        break;

    case PROTO_VR:
        replica = new specpaxos::vr::VRReplica(config, index,
                                               !recover,
                                               &transport,
                                               batchSize,
                                               nullApp);
        break;

    case PROTO_FASTPAXOS:
        replica = new specpaxos::fastpaxos::FastPaxosReplica(config, index,
							     !recover,
                                                             &transport,
                                                             nullApp);
        break;

    case PROTO_SPEC:
        replica = new specpaxos::spec::SpecReplica(config, index,
                                                   !recover,
                                                   &transport, nullApp);
        break;

    case PROTO_NOPAXOS:
        replica = new specpaxos::nopaxos::NOPaxosReplica(config, index,
                                                         !recover,
                                                         &transport,
                                                         nullApp);
	break;

    default:
        NOT_REACHABLE();
    }

    transport.Run();
    delete replica;
}
