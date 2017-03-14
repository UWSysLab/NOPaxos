// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * unreplicated-test.cc:
 *   test cases for unreplicated protocol
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
#include "common/client.h"
#include "common/replica.h"
#include "lib/transport.h"
#include "lib/simtransport.h"
#include "unreplicated/client.h"
#include "unreplicated/replica.h"

#include <stdlib.h>
#include <stdio.h>
#include <gtest/gtest.h>

using namespace specpaxos;
using namespace specpaxos::unreplicated;
using namespace specpaxos::unreplicated::proto;
using std::map;
using std::vector;

static string replicaLastOp;
static string clientLastOp;
static string clientLastReply;
static string replicaLastUnloggedOp;

class UnrepTestApp : public AppReplica {
public:
    UnrepTestApp() { };
    ~UnrepTestApp() { };

    void ReplicaUpcall(opnum_t opnum, const string &req, string &reply,
                       void *arg = nullptr, void *ret = nullptr) override {
        replicaLastOp = req;
        reply = "reply: " + req;
    }

    void UnloggedUpcall(const string &req, string &reply) {
        replicaLastUnloggedOp = req;
        reply = "unlreply: " + req;
    }
};

static void ClientUpcallHandler(const string &req, const string &reply)
{
    clientLastOp = req;
    clientLastReply = reply;
}


TEST(Unreplicated, OneOp)
{
    map<int, vector<ReplicaAddress> > replicaAddrs =
        { {0, {
                  { "localhost", "12345" }
              }
          } };
    Configuration c(1, 1, 0, replicaAddrs);

    SimulatedTransport transport;

    UnrepTestApp app;

    UnreplicatedReplica replica(c, 0, true, &transport, &app);
    UnreplicatedClient client(c, &transport);

    client.Invoke(string("test"), ClientUpcallHandler);

    transport.Run();

    EXPECT_EQ(replicaLastOp, "test");
    EXPECT_EQ(clientLastOp, "test");
    EXPECT_EQ(clientLastReply, "reply: test");
    EXPECT_EQ(replicaLastUnloggedOp, "");
}

TEST(Unreplicated, Unlogged)
{
    map<int, vector<ReplicaAddress> > replicaAddrs =
        { {0, {
                  { "localhost", "12345" }
              }
          } };
    Configuration c(1, 1, 0, replicaAddrs);

    SimulatedTransport transport;
    UnrepTestApp app;

    UnreplicatedReplica replica(c, 0, true, &transport, &app);
    UnreplicatedClient client(c, &transport);

    client.InvokeUnlogged(0, string("test2"), ClientUpcallHandler);

    transport.Run();

    EXPECT_EQ(replicaLastOp, "test");
    EXPECT_EQ(replicaLastUnloggedOp, "test2");
    EXPECT_EQ(clientLastOp, "test2");
    EXPECT_EQ(clientLastReply, "unlreply: test2");
}
