// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * spec-test.cc:
 *   test cases for Speculative Paxos protocol
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

#include "lib/message.h"
#include "lib/transport.h"
#include "lib/simtransport.h"

#include "common/client.h"
#include "lib/configuration.h"
#include "common/log.h"
#include "common/replica.h"
#include "spec/client.h"
#include "spec/replica.h"

#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <sstream>
#include <gtest/gtest.h>

static string replicaLastOp;
static string clientLastOp;
static string clientLastReply;

using google::protobuf::Message;

namespace specpaxos {

using namespace specpaxos::spec;

class SpecTestApp : public AppReplica
{
public:
    SpecTestApp() { };
    ~SpecTestApp() { };
    virtual void ReplicaUpcall(opnum_t opnum, const string &req, string &reply,
                               void *arg = nullptr, void *ret = nullptr) override {
        ops.push_back(req);
        reply = "reply: " + req;
    }

    virtual void RollbackUpcall(opnum_t from, opnum_t to, const std::map<opnum_t, string> &rollbackops) override {
        for (opnum_t x = from; x > to; x--) {
            ops.pop_back();
        }
    }

    virtual void UnloggedUpcall(const string &req, string &reply) override {
        unloggedOps.push_back(req);
        reply = "unlreply: " + req;
    }
    std::vector<string> ops;
    std::vector<string> unloggedOps;

};

class SpecTest : public testing::Test
{
protected:
    std::vector<SpecTestApp *> apps;
    std::vector<SpecReplica *> replicas;
    SpecClient *client;
    SimulatedTransport *transport;
    Configuration *config;
    std::vector<string> replies;
    int requestNum;

    virtual void SetUp() {
        std::map<int, std::vector<ReplicaAddress> > replicaAddrs =
            {
                {0, {
                        { "localhost", "12345" },
                        { "localhost", "12346" },
                        { "localhost", "12347" },
                        { "localhost", "12348" },
                        { "localhost", "12349" }
                    }
                }
            };
        config = new Configuration(1, 5, 2, replicaAddrs);

        transport = new SimulatedTransport();

        for (int i = 0; i < config->n; i++) {
            apps.push_back(new SpecTestApp());
            replicas.push_back(new SpecReplica(*config, i, true, transport, apps[i]));
        }

        client = new SpecClient(*config, transport);
        requestNum = -1;
    }

    virtual string RequestOp(int n) {
        std::ostringstream stream;
        stream << "test: " << n;
        return stream.str();
    }

    virtual string LastRequestOp() {
        return RequestOp(requestNum);
    }

    virtual void ClientSendNext(Client::continuation_t upcall) {
        requestNum++;
        client->Invoke(LastRequestOp(), upcall);
    }

    virtual void ClientSendNextUnlogged(int idx, Client::continuation_t upcall,
                                        Client::timeout_continuation_t timeoutContinuation = nullptr,
                                        uint32_t timeout = Client::DEFAULT_UNLOGGED_OP_TIMEOUT) {
        requestNum++;
        client->InvokeUnlogged(idx, LastRequestOp(), upcall, timeoutContinuation, timeout);
    }

    virtual void TearDown() {
        for (auto x : replicas) {
            delete x;
        }
        for (auto a : apps) {
            delete a;
        }
        apps.clear();
        replicas.clear();

        delete client;
        delete transport;
        delete config;
    }
};

TEST_F(SpecTest, OneOp)
{
    auto upcall = [this](const string &req, const string &reply) {
        EXPECT_EQ(req, LastRequestOp());
        EXPECT_EQ(reply, "reply: "+LastRequestOp());

        // Not guaranteed that any replicas except the leader have
        // executed this request.
        EXPECT_EQ(apps[0]->ops.back(), req);
    };

    // 5 seconds should give synchronization enough time to finish...
    transport->Timer(5000, [&]() {
            transport->CancelAllTimers();
        });

    ClientSendNext(upcall);
    transport->Run();

    // By now, they all should have executed the last request.
    for (int i = 0; i < config->n; i++) {
        EXPECT_EQ(apps[i]->ops.size(), 1);
        EXPECT_EQ(apps[i]->ops.back(),  LastRequestOp());
        const LogEntry *entry = replicas[i]->log.Find(1);
        ASSERT_NE(nullptr, entry);
        EXPECT_EQ(LastRequestOp(), entry->request.op());
        EXPECT_EQ(LOG_STATE_COMMITTED, entry->state);
    }
}

TEST_F(SpecTest, Unlogged)
{
    auto upcall = [this](const string &req, const string &reply) {
        EXPECT_EQ(req, LastRequestOp());
        EXPECT_EQ(reply, "unlreply: "+LastRequestOp());

        EXPECT_EQ(apps[1]->unloggedOps.back(), req);
        transport->CancelAllTimers();
    };
    int timeouts = 0;
    auto timeout = [&](const string &req) {
        timeouts++;
    };

    ClientSendNextUnlogged(1, upcall, timeout);
    transport->Run();

    for (unsigned int i = 0; i < apps.size(); i++) {
        EXPECT_EQ(0, apps[i]->ops.size());
        EXPECT_EQ((i == 1 ? 1 : 0), apps[i]->unloggedOps.size());
    }
    EXPECT_EQ(0, timeouts);
}

TEST_F(SpecTest, UnloggedTimeout)
{
    auto upcall = [this](const string &req, const string &reply) {
        FAIL();
        transport->CancelAllTimers();
    };
    int timeouts = 0;
    auto timeout = [&](const string &req) {
        timeouts++;
    };

    // Drop messages to or from replica 1
    transport->AddFilter(10, [](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
                             if ((srcIdx.second == 1) || (dstIdx.second == 1)) {
                                 return false;
                             }
                             return true;
                         });

    // Run for 10 seconds
    transport->Timer(10000, [&]() {
            transport->CancelAllTimers();
        });

    ClientSendNextUnlogged(1, upcall, timeout);
    transport->Run();

    for (unsigned int i = 0; i < apps.size(); i++) {
        EXPECT_EQ(0, apps[i]->ops.size());
        EXPECT_EQ(0, apps[i]->unloggedOps.size());
    }
    EXPECT_EQ(1, timeouts);
}


TEST_F(SpecTest, ManyOps)
{
    Client::continuation_t upcall = [&](const string &req, const string &reply) {
        EXPECT_EQ(req, LastRequestOp());
        EXPECT_EQ(reply, "reply: "+LastRequestOp());

        // Not guaranteed that any replicas except the leader have
        // executed this request.
        EXPECT_EQ(apps[0]->ops.back(), req);

        if (requestNum < 9) {
            ClientSendNext(upcall);
        }
    };

    // 5 seconds should give synchronization enough time to finish...
    transport->Timer(5000, [&]() {
            transport->CancelAllTimers();
        });

    ClientSendNext(upcall);
    transport->Run();

    // By now, they all should have executed the last request.
    for (int i = 0; i < config->n; i++) {
        EXPECT_EQ(10, apps[i]->ops.size());
        for (int j = 0; j < 10; j++) {
            EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);
            const LogEntry *entry = replicas[i]->log.Find(j+1);
            ASSERT_NE(nullptr, entry);
            EXPECT_EQ(RequestOp(j), entry->request.op());
            EXPECT_EQ(LOG_STATE_COMMITTED, entry->state);
        }
    }
}

TEST_F(SpecTest, FailedReplica)
{
    Client::continuation_t upcall = [&](const string &req, const string &reply) {
        EXPECT_EQ(req, LastRequestOp());
        EXPECT_EQ(reply, "reply: "+LastRequestOp());

        // Not guaranteed that any replicas except the leader have
        // executed this request.
        EXPECT_EQ(apps[0]->ops.back(), req);

        if (requestNum < 9) {
            ClientSendNext(upcall);
        }
    };

    // 5 seconds should give synchronization enough time to finish...
    transport->Timer(5000, [&]() {
            transport->CancelAllTimers();
        });

    ClientSendNext(upcall);

    // Drop messages to or from replica 1
    transport->AddFilter(10, [](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
                             if ((srcIdx.second == 1) || (dstIdx.second == 1)) {
                                 return false;
                             }
                             return true;
                         });

    transport->Run();

    // By now, they all should have executed the last request.
    for (int i = 0; i < config->n; i++) {
        if (i == 1) {
            continue;
        }
        EXPECT_EQ(10, apps[i]->ops.size());
        for (int j = 0; j < 10; j++) {
            EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);
            const LogEntry *entry = replicas[i]->log.Find(j+1);
            ASSERT_NE(nullptr, entry);
            EXPECT_EQ(RequestOp(j), entry->request.op());
            EXPECT_EQ(LOG_STATE_COMMITTED, entry->state);
        }
    }
}

TEST_F(SpecTest, StateTransfer)
{
    Client::continuation_t upcall = [&](const string &req, const string &reply) {
        EXPECT_EQ(req, LastRequestOp());
        EXPECT_EQ(reply, "reply: "+LastRequestOp());

        // Not guaranteed that any replicas except the leader have
        // executed this request.
        EXPECT_EQ(apps[0]->ops.back(), req);

        if (requestNum == 5) {
            // Restore replica 3
            transport->RemoveFilter(10);
        }

        if (requestNum < 9) {
            ClientSendNext(upcall);
        }
    };

    // 15 seconds should give synchronization enough time to finish...
    // (There's currently a client timeout in there.)
    transport->Timer(15000, [&]() {
            transport->CancelAllTimers();
        });

    ClientSendNext(upcall);

    // Drop messages to or from replica 3
    transport->AddFilter(10, [](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
                             if ((srcIdx.second == 3) || (dstIdx.second == 3)) {
                                 return false;
                             }
                             return true;
                         });

    transport->Run();

    // By now, they all should have executed the last request.
    for (int i = 0; i < config->n; i++) {
        EXPECT_EQ(10, apps[i]->ops.size());
        for (int j = 0; j < 10; j++) {
            EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);
        }
    }
}

TEST_F(SpecTest, FailedLeader)
{
    Client::continuation_t upcall = [&](const string &req, const string &reply) {
        EXPECT_EQ(req, LastRequestOp());
        EXPECT_EQ(reply, "reply: "+LastRequestOp());

        if (requestNum == 5) {
            // Drop messages to or from replica 0
            transport->AddFilter(10, [](TransportReceiver *src, std::pair<int, int> srcIdx,
                                        TransportReceiver *dst, std::pair<int, int> dstIdx,
                                        Message &m, uint64_t &delay) {
                                     if ((srcIdx.second == 0) || (dstIdx.second == 0)) {
                                         return false;
                                     }
                                     return true;
                                 });
        }
        if (requestNum < 9) {
            ClientSendNext(upcall);
        }
    };

    // 15 seconds should give synchronization enough time to finish...
    transport->Timer(15000, [&]() {
            transport->CancelAllTimers();
        });

    ClientSendNext(upcall);

    transport->Run();

    // By now, they all should have executed the last request.
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            continue;
        }
        EXPECT_EQ(10, apps[i]->ops.size());
        for (int j = 0; j < 10; j++) {
            EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);
        }
    }
}

TEST_F(SpecTest, Conflict)
{
    Client::continuation_t upcall = [&](const string &req, const string &reply) {
    };

    // This one needs two clients. Set up a second one.
    SpecClient otherClient(*config, transport);

    // Delay messages from the first client to two of the replicas
    transport->AddFilter(10, [=](TransportReceiver *src, std::pair<int, int> srcIdx,
                                 TransportReceiver *dst, std::pair<int, int> dstIdx,
                                 Message &m, uint64_t &delay) {
                             if ((src == client) &&
                                 (dstIdx.second < 2)) {
                                 delay = 100;
                             }
                             return true;
                         });

    // Fire off the first client immediately and the second client in
    // 10 ms, so that the replicas receive in a different order.
    client->Invoke("A", upcall);

    transport->Timer(10, [&]() {
        otherClient.Invoke("B", upcall);
    });

    // 15 seconds should give synchronization enough time to finish...
    transport->Timer(15000, [&]() {
            transport->CancelAllTimers();
        });

    transport->Run();

    for (int i = 0; i < config->n; i++) {
        EXPECT_EQ(2, apps[i]->ops.size());
    }
}

TEST_F(SpecTest, ImmediatelyFailedLeader)
{
    Client::continuation_t upcall = [&](const string &req, const string &reply) {
        EXPECT_EQ(req, LastRequestOp());
        EXPECT_EQ(reply, "reply: "+LastRequestOp());

        if (requestNum == 5) {
        }
        if (requestNum < 9) {
            ClientSendNext(upcall);
        }
    };

    // 45 seconds should give synchronization enough time to finish...
    transport->Timer(45000, [&]() {
            transport->CancelAllTimers();
        });

    // Drop messages to or from replica 0
    transport->AddFilter(10, [](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
                             if ((srcIdx.second == 0) || (dstIdx.second == 0)) {
                                 return false;
                             }
                             return true;
                         });

    // ...and restore it after 30 seconds.
    transport->Timer(30000, [&]() {
            transport->RemoveFilter(10);
        });

    // Don't send request untl 15 seconds in.
    transport->Timer(15000, [&]() {
            ClientSendNext(upcall);
        });

    transport->Run();

    // By now, they all should have executed all requests
    for (int i = 0; i < config->n; i++) {
        ASSERT_EQ(10, apps[i]->ops.size());
        for (int j = 0; j < 10; j++) {
            EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);
        }
    }
}

TEST_F(SpecTest, NoSuperquorum)
{
    Client::continuation_t upcall = [&](const string &req, const string &reply) {
        EXPECT_EQ(req, LastRequestOp());
        EXPECT_EQ(reply, "reply: "+LastRequestOp());

        // Not guaranteed that any replicas except the leader have
        // executed this request.
        EXPECT_EQ(apps[0]->ops.back(), req);

        if (requestNum < 9) {
            ClientSendNext(upcall);
        }
    };

    // 300 seconds should be enough time for the slow path
    transport->Timer(300000, [&]() {
            transport->CancelAllTimers();
        });

    ClientSendNext(upcall);

    // Drop messages to or from replicas 1 and 2
    transport->AddFilter(10, [](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
                             if ((srcIdx.second == 1) || (dstIdx.second == 1)) {
                                 return false;
                             }
                             if ((srcIdx.second == 2) || (dstIdx.second == 2)) {
                                 return false;
                             }
                             return true;
                         });

    transport->Run();

    // By now, they all should have executed the last request.
    for (int i = 0; i < config->n; i++) {
        if ((i == 1) || (i == 2)) {
            continue;
        }
        ASSERT_EQ(10, apps[i]->ops.size());
        for (int j = 0; j < 10; j++) {
            EXPECT_EQ(RequestOp(j), apps[i]->ops[j]);
            const LogEntry *entry = replicas[i]->log.Find(j+1);
            ASSERT_NE(nullptr, entry);
            EXPECT_EQ(RequestOp(j), entry->request.op());
            EXPECT_EQ(LOG_STATE_COMMITTED, entry->state);
        }
    }
}

TEST_F(SpecTest, Stress)
{
    const int NUM_CLIENTS = 10;
    const int MAX_REQS = 1000;
    const int MAX_DELAY = 1000;
    const int DROP_PROBABILITY = 5; // 1/x

    std::vector<SpecClient *> clients;
    std::vector<int> lastReq;
    std::vector<Client::continuation_t> upcalls;
    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(new SpecClient(*config, transport));
        lastReq.push_back(0);
        upcalls.push_back([&, i](const string &req, const string &reply) {
                EXPECT_EQ("reply: "+RequestOp(lastReq[i]), reply);
                lastReq[i] += 1;
                if (lastReq[i] < MAX_REQS) {
                    clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
                }
            });
        clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
    }

    srand(time(NULL));

    // Delay messages from clients by a random amount, and drop some
    // of them
    transport->AddFilter(10, [=](TransportReceiver *src, std::pair<int, int> srcIdx,
                                 TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
                             if (srcIdx.second == -1) {
                                 delay = rand() % MAX_DELAY;
                             }
                             return ((rand() % DROP_PROBABILITY) != 0);
                         });

    // This could take a while; simulate two hours
    transport->Timer(7200000, [&]() {
            transport->CancelAllTimers();
        });

    transport->Run();

    for (int i = 0; i < config->n; i++) {
        ASSERT_EQ(NUM_CLIENTS * MAX_REQS, apps[i]->ops.size());
    }

    for (int i = 0; i < NUM_CLIENTS*MAX_REQS; i++) {
        for (int j = 0; j < config->n; j++) {
            ASSERT_EQ(apps[0]->ops[i], apps[j]->ops[i]);
        }
    }

    for (SpecClient *c : clients) {
        delete c;
    }
}

} // namespace specpaxos
