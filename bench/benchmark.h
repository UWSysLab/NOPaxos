// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * benchmark.h:
 *   simple replication benchmark client
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

#include "common/client.h"
#include "lib/latency.h"
#include "lib/transport.h"

namespace specpaxos {

class BenchmarkClient
{
public:
    BenchmarkClient(Client &client, Transport &transport,
                    int numRequests, uint64_t delay,
                    int warmupSec,
		    int tputInterval,
                    string latencyFilename = "");
    void Start();
    void OnReply(const string &request, const string &reply);
    struct Latency_t latency;
    bool started;
    bool done;
    bool cooldownDone;
    int tputInterval;
    std::vector<uint64_t> latencies;

private:
    void SendNext();
    void Finish();
    void WarmupDone();
    void CooldownDone();
    void TimeInterval();
    Client &client;
    Transport &transport;
    int numRequests;
    uint64_t delay;
    int n;
    int warmupSec;
    struct timeval startTime;
    struct timeval endTime;
    string latencyFilename;
    int msSinceStart;
    int opLastInterval;
};

} // namespace specpaxos
