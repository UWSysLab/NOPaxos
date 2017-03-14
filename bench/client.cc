// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * client.cpp:
 *   test instantiation of a client application
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

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/udptransport.h"

#include "bench/benchmark.h"
#include "common/client.h"
#include "lib/configuration.h"
#include "fastpaxos/client.h"
#include "spec/client.h"
#include "unreplicated/client.h"
#include "vr/client.h"
#include "nopaxos/client.h"

#include <unistd.h>
#include <stdlib.h>
#include <fstream>

static void
Usage(const char *progName)
{
        fprintf(stderr, "usage: %s [-n requests] [-t threads] [-w warmup-secs] [-l latency-file] [-q dscp] [-d delay-ms] -c conf-file -m unreplicated|vr|fastpaxos|spec|nopaxos\n",
                progName);
        exit(1);
}

void
PrintReply(const string &request, const string &reply)
{
    Notice("Request succeeded; got response %s", reply.c_str());
}

int main(int argc, char **argv)
{
    const char *configPath = NULL;
    int numClients = 1;
    int numRequests = 100;
    int warmupSec = 0;
    int dscp = 0;
    uint64_t delay = 0;
    int tputInterval = 0;

    enum
    {
        PROTO_UNKNOWN,
        PROTO_UNREPLICATED,
        PROTO_VR,
        PROTO_FASTPAXOS,
        PROTO_SPEC,
	PROTO_NOPAXOS
    } proto = PROTO_UNKNOWN;

    string latencyFile;
    string latencyRawFile;

    // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "c:d:q:l:m:n:t:w:i:")) != -1) {
        switch (opt) {
        case 'c':
            configPath = optarg;
            break;

        case 'd':
        {
            char *strtolPtr;
            delay = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -d requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

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

        case 'l':
            latencyFile = string(optarg);
            break;

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
	    }
	    else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                Usage(argv[0]);
            }
            break;

        case 'n':
        {
            char *strtolPtr;
            numRequests = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (numRequests <= 0))
            {
                fprintf(stderr,
                        "option -n requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 't':
        {
            char *strtolPtr;
            numClients = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (numClients <= 0))
            {
                fprintf(stderr,
                        "option -t requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        case 'w':
        {
            char *strtolPtr;
            warmupSec = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (numRequests <= 0))
            {
                fprintf(stderr,
                        "option -w requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

	case 'i':
        {
            char *strtolPtr;
            tputInterval = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -d requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }


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
    if (proto == PROTO_UNKNOWN) {
        fprintf(stderr, "option -m is required\n");
        Usage(argv[0]);
    }

    // Load configuration
    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                configPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration config(configStream);

    UDPTransport transport(0, 0, dscp);
    std::vector<specpaxos::Client *> clients;
    std::vector<specpaxos::BenchmarkClient *> benchClients;

    for (int i = 0; i < numClients; i++) {
        specpaxos::Client *client;
        switch (proto) {
        case PROTO_UNREPLICATED:
            client =
                new specpaxos::unreplicated::UnreplicatedClient(config,
                                                                &transport);
            break;

        case PROTO_VR:
            client = new specpaxos::vr::VRClient(config, &transport);
            break;

        case PROTO_FASTPAXOS:
            client = new specpaxos::fastpaxos::FastPaxosClient(config,
                                                               &transport);
            break;

        case PROTO_SPEC:
            client = new specpaxos::spec::SpecClient(config, &transport);
            break;

	case PROTO_NOPAXOS:
	    client = new specpaxos::nopaxos::NOPaxosClient(config, &transport);
	    break;

        default:
            NOT_REACHABLE();
        }

        specpaxos::BenchmarkClient *bench =
            new specpaxos::BenchmarkClient(*client, transport,
                                           numRequests, delay,
                                           warmupSec, tputInterval);

        transport.Timer(0, [=]() { bench->Start(); });
        clients.push_back(client);
        benchClients.push_back(bench);
    }

    Timeout checkTimeout(&transport, 100, [&]() {
            for (auto x : benchClients) {
                if (!x->cooldownDone) {
                    return;
                }
            }
            Notice("All clients done.");

            Latency_t sum;
            _Latency_Init(&sum, "total");
            for (unsigned int i = 0; i < benchClients.size(); i++) {
                Latency_Sum(&sum, &benchClients[i]->latency);
            }
            Latency_Dump(&sum);
            if (latencyFile.size() > 0) {
                Latency_FlushTo(latencyFile.c_str());
            }

            latencyRawFile = latencyFile+".raw";
            std::ofstream rawFile(latencyRawFile.c_str(),
                                  std::ios::out | std::ios::binary);
            for (auto x : benchClients) {
                rawFile.write((char *)&x->latencies[0],
                              (x->latencies.size()*sizeof(x->latencies[0])));
                if (!rawFile) {
                    Warning("Failed to write raw latency output");
                }
            }
            exit(0);
        });
    checkTimeout.Start();

    transport.Run();
}
