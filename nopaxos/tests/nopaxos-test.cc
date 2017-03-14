// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * nopaxos-test.cc:
 *   test cases for Network-Ordered Paxos
 *
 * Copyright 2016 Jialin Li <lijl@cs.washington.edu>
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
#include "lib/message.h"
#include "lib/simtransport.h"

#include "nopaxos/client.h"
#include "nopaxos/replica.h"

#include <stdio.h>
#include <stdlib.h>
#include <gtest/gtest.h>

using google::protobuf::Message;
using namespace specpaxos;
using namespace specpaxos::nopaxos;
using namespace specpaxos::nopaxos::proto;

class NOPaxosTestApp : public AppReplica
{
public:
    NOPaxosTestApp() { };
    ~NOPaxosTestApp() { };

    void ReplicaUpcall(opnum_t opnum, const string &req, string &reply,
                       void *arg = nullptr, void *ret = nullptr) override {
        ops.push_back(req);
        reply = "reply: " + req;
    }

    void UnloggedUpcall(const string &req, string &reply) override {
        unloggedOps.push_back(req);
        reply = "unreply: " + req;
    }

    std::vector<string> ops;
    std::vector<string> unloggedOps;
};

class NOPaxosTest : public ::testing::Test
{
protected:
    struct TestClient {
        NOPaxosClient *client;
        int requestNum;

        TestClient()
            : requestNum(0) {}

        TestClient(NOPaxosClient *client)
            : client(client), requestNum(0) {}

        string RequestOp(int n) {
            std::ostringstream stream;
            stream << "test: " << n;
            return stream.str();
        }

        string LastRequestOp() {
            return RequestOp(requestNum);
        }

        void SendNext(Client::continuation_t upcall) {
            requestNum++;
            client->Invoke(LastRequestOp(), upcall);
        }

        void SendNextUnlogged(int idx, Client::continuation_t upcall,
                              Client::timeout_continuation_t timeoutContinuation = nullptr,
                              uint32_t timeout = Client::DEFAULT_UNLOGGED_OP_TIMEOUT) {
            requestNum++;
            client->InvokeUnlogged(idx, LastRequestOp(), upcall, timeoutContinuation, timeout);
        }
    };

    std::vector<NOPaxosTestApp *> apps;
    std::vector<NOPaxosReplica *> replicas;
    std::vector<int> clientMsgCounter;
    SimulatedTransport *transport;
    Configuration *config;

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
            apps.push_back(new NOPaxosTestApp());
            replicas.push_back(new NOPaxosReplica(*config, i, true, transport, apps[i]));
            clientMsgCounter.push_back(0);
        }
    }

    virtual void TearDown() {
        for (auto x : replicas) {
            delete x;
        }
        for (auto x : apps) {
            delete x;
        }
        apps.clear();
        replicas.clear();

        delete transport;
        delete config;
    }
};

TEST_F(NOPaxosTest, OneOp)
{
    int numUpcalls = 0;
    NOPaxosClient nopaxosClient(*config, transport);
    TestClient client(&nopaxosClient);

    auto upcall = [&](const string & request, const string & reply) {
        numUpcalls++;

        EXPECT_EQ(client.LastRequestOp(), request);
        EXPECT_EQ("reply: " + request, reply);

        // Leader should have executed the request
        EXPECT_EQ(1, apps[0]->ops.size());
        EXPECT_EQ(request, apps[0]->ops.back());

        // There should be a quorum of replicas received this request
        int numReceived = 0;
        for (int i = 0; i < config->n; i++) {
            const LogEntry *entry = replicas[i]->log.Find(1);
            if (entry != nullptr) {
                numReceived++;
                EXPECT_EQ(client.LastRequestOp(), entry->request.op());
                EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);
            }
        }
        EXPECT_GE(numReceived, config->QuorumSize());
    };

    // Without any failures, 100ms should be enough for all replicas to receive the request
    transport->Timer(100, [&]() {
        transport->CancelAllTimers();
    });

    client.SendNext(upcall);
    transport->Run();

    // By now, all replicas should have received the request
    for (int i = 0; i < config->n; i++) {
        const LogEntry *entry = replicas[i]->log.Find(1);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(client.LastRequestOp(), entry->request.op());
        EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);

        if (i == 0) {
            EXPECT_EQ(1, apps[i]->ops.size());
            EXPECT_EQ(client.LastRequestOp(), apps[i]->ops.back());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }
    }
    // And client should have made the upcall
    EXPECT_EQ(1, numUpcalls);
}

TEST_F(NOPaxosTest, OneOpUnlogged)
{
    NOPaxosClient nopaxosClient(*config, transport);
    TestClient client(&nopaxosClient);

    auto upcall = [&](const string & request, const string &reply) {
        EXPECT_EQ(request, client.LastRequestOp());
        EXPECT_EQ(reply, "unreply: " + request);

        EXPECT_EQ(apps[1]->unloggedOps.back(), request);
        transport->CancelAllTimers();
    };
    int timeouts = 0;
    auto timeout = [&](const string & request) {
        timeouts++;
    };

    client.SendNextUnlogged(1, upcall, timeout);
    transport->Run();

    for (unsigned int i = 0; i < apps.size(); i++) {
        EXPECT_EQ(0, apps[i]->ops.size());
        EXPECT_EQ((i == 1 ? 1 : 0), apps[i]->unloggedOps.size());
    }
    EXPECT_EQ(0, timeouts);
}

TEST_F(NOPaxosTest, ManyOps)
{
    NOPaxosClient nopaxosClient(*config, transport);
    TestClient client(&nopaxosClient);

    int numUpcalls = 0;
    Client::continuation_t upcall = [&](const string & request, const string & reply) {
        numUpcalls++;

        EXPECT_EQ(client.LastRequestOp(), request);
        EXPECT_EQ("reply: " + request, reply);

        // Leader should have executed the request
        EXPECT_EQ(client.requestNum, apps[0]->ops.size());
        EXPECT_EQ(request, apps[0]->ops.back());

        // There should be a quorum of replicas received this request
        int numReceived = 0;
        for (int i = 0; i < config->n; i++) {
            const LogEntry *entry = replicas[i]->log.Find(client.requestNum);
            if (entry != nullptr) {
                numReceived++;
                EXPECT_EQ(client.LastRequestOp(), entry->request.op());
                EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);
            }
        }
        EXPECT_GE(numReceived, config->QuorumSize());

        if (client.requestNum < 10) {
            client.SendNext(upcall);
        }
    };

    // Without any failures, 100ms should be enough for all replicas to receive the request
    transport->Timer(100, [&]() {
        transport->CancelAllTimers();
    });

    client.SendNext(upcall);
    transport->Run();

    // By now, all replicas should have received the request
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(10, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }

        for (int j = 1; j <= 10; j++) {
            const LogEntry *entry = replicas[i]->log.Find(j);
            ASSERT_NE(entry, nullptr);
            EXPECT_EQ(client.RequestOp(j), entry->request.op());
            EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);

            if (i == 0) {
                EXPECT_EQ(client.RequestOp(j), apps[i]->ops[j-1]);
            }
        }
    }
    // And client should have made the upcall 10 times
    EXPECT_EQ(10, numUpcalls);
}

TEST_F(NOPaxosTest, ReplicaGap)
{
    const int NUM_CLIENTS = 2;
    const int NUM_PACKETS = 6;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 3rd, 5th and 8th packet from the client to non-leader replicas
    // Replicas upon receive the next packet, will learn from the leader
    std::set<int> drops = {3, 5, 8};
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second != 0) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 100ms should be enough for all replicas to receive the request
    transport->Timer(100, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // By now, all replicas should have received the requests
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }

        if (i > 0) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS; j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);
                EXPECT_EQ(LOG_STATE_RECEIVED, leaderEntry->state);
            }
        }
    }
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);

    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, LeaderGap)
{
    const int NUM_CLIENTS = 2;
    const int NUM_PACKETS = 6;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 3rd, 5th and 8th packet from the client to the leader,
    // will not initiate the gap agreement protocol
    std::set<int> drops = {3, 5, 8};
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second == 0) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 100ms should be enough for all replicas to receive the request
    transport->Timer(100, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // By now, all replicas should have received the requests
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }

        if (i > 0) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS; j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);
                EXPECT_EQ(LOG_STATE_RECEIVED, leaderEntry->state);
            }
        }
    }
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, LeaderReplicaGap)
{
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 4;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 5th, 6th and 10th packet from the client to the leader,
    // and a replica.
    std::set<int> drops = {5, 6, 10};

    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && (dstIdx.second == 0 || dstIdx.second == 1)) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 100ms should be enough for all replicas to receive the request
    transport->Timer(100, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // By now, all replicas should have received the requests
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }

        if (i > 0) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS; j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);
                EXPECT_EQ(LOG_STATE_RECEIVED, leaderEntry->state);
            }
        }
    }
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, CommittedGap)
{
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 4;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 5th and 10th packet from the client to all replicas.
    // This forces gap agreement protocol.
    std::set<int> drops = {5, 10};

    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second >= 0) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 500ms should be enough for all replicas to receive the request
    transport->Timer(500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // All replicas should have received the requests. There should
    // be two committed gaps in the log.
    std::set<opnum_t> committedGaps;
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }

        if (i > 0) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS+2; j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(entry->state, leaderEntry->state);
                EXPECT_TRUE(entry->state == LOG_STATE_RECEIVED || entry->state == LOG_STATE_NOOP);
                if (entry->state == LOG_STATE_RECEIVED) {
                    EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                } else {
                    committedGaps.insert(j);
                }
            }
        }
    }
    EXPECT_EQ(committedGaps.size(), 2);
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, ReplicaGapRequestTimeout)
{
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 4;
    const int CLIENT_DROP_PROBABILITY = 4;
    const int GAPREQ_DROP_PROBABILITY = 2;


    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Randomly drop packet from the client to non-leader replicas.
    // Also randomly drop GapRequestMessage from replica to leaders.
    // No gap agreement protocol will happen.
    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    GapRequestMessage gapRequestMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second > 0) {
            if (rand() % CLIENT_DROP_PROBABILITY == 0) {
                return false;
            }
        } else if (srcIdx.second > 0 && dstIdx.second == 0
                   && m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            if (rand() % GAPREQ_DROP_PROBABILITY == 0) {
                return false;
            }
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 500ms should be enough for all replicas to receive the request
    transport->Timer(500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // All replicas should have received the requests. There should
    // be no committed gaps in the log.
    EXPECT_GE(replicas[0]->log.LastOpnum(), NUM_CLIENTS*NUM_PACKETS);
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }

        if (i > 0) {
            for (unsigned int j = 1; j <= replicas[i]->log.LastOpnum(); j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(entry->state, leaderEntry->state);
                EXPECT_TRUE(entry->state == LOG_STATE_RECEIVED);
                EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
            }
        }
    }
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, LeaderGapRequestTimeout)
{
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 4;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 5th and 10th packet from the client to the leader.
    // Also drop all GapRequestMessage from leader to replicas.
    // This forces gap agreement protocol.
    std::set<int> drops = {5, 10};
    GapRequestMessage gapRequestMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second == 0) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        } else if (srcIdx.second == 0 && dstIdx.second > 0
                   && m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            return false;
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 500ms should be enough for all replicas to receive the request
    transport->Timer(500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // All replicas should have received the requests. There should
    // be two committed gaps in the log.
    std::set<opnum_t> committedGaps;
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }
        EXPECT_EQ(replicas[i]->log.LastOpnum(), NUM_CLIENTS*NUM_PACKETS+2);

        if (i > 0) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS+2; j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(entry->state, leaderEntry->state);
                EXPECT_TRUE(entry->state == LOG_STATE_RECEIVED || entry->state == LOG_STATE_NOOP);
                if (entry->state == LOG_STATE_RECEIVED) {
                    EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                } else {
                    committedGaps.insert(j);
                }
            }
        }
    }
    EXPECT_EQ(committedGaps.size(), 2);
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, GapCommitTimeout)
{
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 4;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 5th and 10th packet from the client to the leader
    // and a replica. Also drop all GapRequest from leader, so it will
    // force a gap commit. Drop all GapCommit for the first round
    // and drop all GapCommitReply for the 2nd GapCommit.
    std::set<int> drops = {5, 10};
    int gapCommitCounter = 0;
    int gapCommitReplyCounter = 0;
    GapRequestMessage gapRequestMessage;
    GapCommitMessage gapCommitMessage;
    GapCommitReplyMessage gapCommitReplyMessage;

    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && (dstIdx.second == 0 || dstIdx.second == 1)) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        } else if (srcIdx.second == 0 &&
                   m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            return false;
        } else if (srcIdx.second == 0 &&
                   m.GetTypeName() == gapCommitMessage.GetTypeName()) {
            gapCommitCounter++;
            if (gapCommitCounter <= 4) {
                return false;
            }
        } else if (srcIdx.second > 0 &&
                   m.GetTypeName() == gapCommitReplyMessage.GetTypeName()) {
            gapCommitReplyCounter++;
            if (gapCommitReplyCounter >= 5 && gapCommitReplyCounter <= 8) {
                return false;
            }
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 500ms should be enough for all replicas to receive the request
    transport->Timer(500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // There should be 2 committed gaps in the logs
    std::set<opnum_t> committedGaps;
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }

        if (i > 0) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS+2; j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(entry->state, leaderEntry->state);
                EXPECT_TRUE(entry->state == LOG_STATE_RECEIVED || entry->state == LOG_STATE_NOOP);
                if (entry->state == LOG_STATE_RECEIVED) {
                    EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                } else {
                    committedGaps.insert(j);
                }
            }
        }
    }
    EXPECT_EQ(committedGaps.size(), 2);
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, RandomGap)
{
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 4;
    const int CLIENT_DROP_PROBABILITY = 4;
    const int GAPMSG_DROP_PROBABILITY = 3;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Randomly drop packets from the client to replicas.
    // Also randomly drop GapRequestMessage and GapReplyMessage.
    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    GapRequestMessage gapRequestMessage;
    GapReplyMessage gapReplyMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second >= 0) {
            if (rand() % CLIENT_DROP_PROBABILITY == 0) {
                return false;
            }
        } else if (m.GetTypeName() == gapRequestMessage.GetTypeName() ||
                   m.GetTypeName() == gapReplyMessage.GetTypeName()) {
            if (rand() % GAPMSG_DROP_PROBABILITY == 0) {
                return false;
            }
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 800ms should be enough for all replicas to receive the request
    transport->Timer(800, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    EXPECT_GE(replicas[0]->log.LastOpnum(), NUM_CLIENTS*NUM_PACKETS);
    for (int i = 0; i < config->n; i++) {
        if (i == 0) {
            EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        } else {
            // Only leader should have executed the request
            EXPECT_EQ(0, apps[i]->ops.size());
        }

        if (i > 0) {
            for (unsigned int j = 1; j <= replicas[i]->log.LastOpnum(); j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(entry->state, leaderEntry->state);
                EXPECT_TRUE(entry->state == LOG_STATE_RECEIVED || entry->state == LOG_STATE_NOOP);
                if (entry->state == LOG_STATE_RECEIVED) {
                    EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                }
            }
        }
    }
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, Synchronization)
{
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 4;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 5th and 10th packet from clients to the leader.
    // Also drop all GapRequestMessage from leader to replicas.
    // This forces gap agreement protocol. Drop GapCommit to 2 of
    // the replicas so synchronization will overwrite them with
    // NOOP.
    std::set<int> drops = {5, 10};
    GapRequestMessage gapRequestMessage;
    GapCommitMessage gapCommitMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second == 0) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        } else if (srcIdx.second == 0 && dstIdx.second > 0
                   && m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            return false;
        } else if (srcIdx.second == 0 && (dstIdx.second == 1 || dstIdx.second == 2)
                   && m.GetTypeName() == gapCommitMessage.GetTypeName()) {
            return false;
        }
        return true;
    });

    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                clients[i].SendNext(upcalls[i]);
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 2000ms is enough to trigger at least 1 synchronization
    transport->Timer(2000, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // All replicas should have received the requests. There should
    // be two committed gaps in the log.
    std::set<opnum_t> committedGaps;
    for (int i = 0; i < config->n; i++) {
        // Synchronization should ensure all replicas have
        // executed all requests
        EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        EXPECT_EQ(replicas[i]->log.LastOpnum(), NUM_CLIENTS*NUM_PACKETS+2);

        if (i > 0) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS+2; j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(entry->state, leaderEntry->state);
                EXPECT_TRUE(entry->state == LOG_STATE_RECEIVED || entry->state == LOG_STATE_NOOP);
                if (entry->state == LOG_STATE_RECEIVED) {
                    EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                } else {
                    committedGaps.insert(j);
                }
            }
        }
    }
    EXPECT_EQ(committedGaps.size(), 2);
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, ViewChange) {
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 8;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 5th and 10th packet from clients to the leader.
    // Also drop all GapRequestMessage from leader to replicas.
    // This forces gap agreement protocol. Drop GapCommit to 2 of
    // the replicas.
    // Drop all SyncPrepare from the 1st leader to trigger view change.
    std::set<int> drops = {5, 10};
    GapRequestMessage gapRequestMessage;
    GapCommitMessage gapCommitMessage;
    SyncPrepareMessage syncPrepareMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second == 0) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        } else if (srcIdx.second == 0 && dstIdx.second > 0
                   && m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            return false;
        } else if (srcIdx.second == 0 && (dstIdx.second == 1 || dstIdx.second == 2)
                   && m.GetTypeName() == gapCommitMessage.GetTypeName()) {
            return false;
        } else if (srcIdx.second == 0 && dstIdx.second > 0
                   && m.GetTypeName() == syncPrepareMessage.GetTypeName()) {
            return false;
        }
        return true;
    });

    // Client sends last 4 requests in the second view
    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                if (clients[i].requestNum == 4) {
                    transport->Timer(2500, [&, i]() {
                        clients[i].SendNext(upcalls[i]);
                    });
                } else {
                    clients[i].SendNext(upcalls[i]);
                }
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // 4000ms is enough to trigger viewchange
    transport->Timer(4000, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // All replicas should have received the requests. There should
    // be two committed gaps in the log.
    std::set<opnum_t> committedGaps;
    for (int i = 0; i < config->n; i++) {
        // Synchronization should ensure all replicas have
        // executed all requests
        EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        EXPECT_EQ(replicas[i]->log.LastOpnum(), NUM_CLIENTS*NUM_PACKETS+2);

        if (i != 1) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS+2; j++) {
                const LogEntry *leaderEntry = replicas[1]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(entry->state, leaderEntry->state);
                EXPECT_TRUE(entry->state == LOG_STATE_RECEIVED || entry->state == LOG_STATE_NOOP);
                if (entry->state == LOG_STATE_RECEIVED) {
                    EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                } else {
                    committedGaps.insert(j);
                }
            }
        }
    }
    EXPECT_EQ(committedGaps.size(), 2);
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}

TEST_F(NOPaxosTest, SessionChange) {
    const int NUM_CLIENTS = 4;
    const int NUM_PACKETS = 8;

    std::vector<TestClient> clients;
    std::vector<Client::continuation_t> upcalls;
    int numUpcalls = 0;

    // Drop the 5th and 10th packet from clients to the leader.
    // Also drop all GapRequestMessage from leader to replicas.
    // This forces gap agreement protocol. Drop GapCommit to 2 of
    // the replicas.
    std::set<int> drops = {5, 10};
    GapRequestMessage gapRequestMessage;
    GapCommitMessage gapCommitMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx.second == -1 && dstIdx.second == 0) {
            clientMsgCounter[dstIdx.second]++;
            if (drops.find(clientMsgCounter[dstIdx.second]) != drops.end()) {
                return false;
            }
        } else if (srcIdx.second == 0 && dstIdx.second > 0
                   && m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            return false;
        } else if (srcIdx.second == 0 && (dstIdx.second == 1 || dstIdx.second == 2)
                   && m.GetTypeName() == gapCommitMessage.GetTypeName()) {
            return false;
        }
        return true;
    });

    // Client sends last 6 requests in the second view
    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.push_back(TestClient(new NOPaxosClient(*config, transport)));
        upcalls.push_back([&, i](const string &req, const string &reply) {
            numUpcalls++;
            EXPECT_EQ(clients[i].LastRequestOp(), req);
            EXPECT_EQ("reply: " + clients[i].LastRequestOp(), reply);
            if (clients[i].requestNum < NUM_PACKETS) {
                if (clients[i].requestNum == 2) {
                    transport->Timer(800, [&, i]() {
                        clients[i].SendNext(upcalls[i]);
                    });
                } else {
                    clients[i].SendNext(upcalls[i]);
                }
            }
        });
        clients[i].SendNext(upcalls[i]);
    }

    // Simulate sequencer failure at 500ms
    transport->Timer(500, [&]() {
        transport->SessionChange();
    });

    transport->Timer(2000, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // All replicas should have received the requests. There should
    // be two committed gaps in the log.
    std::set<opnum_t> committedGaps;
    for (int i = 0; i < config->n; i++) {
        // Synchronization should ensure all replicas have
        // executed all requests
        EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, apps[i]->ops.size());
        EXPECT_EQ(replicas[i]->log.LastOpnum(), NUM_CLIENTS*NUM_PACKETS+2);

        if (i != 0) {
            for (int j = 1; j <= NUM_CLIENTS*NUM_PACKETS+2; j++) {
                const LogEntry *leaderEntry = replicas[0]->log.Find(j);
                const LogEntry *entry = replicas[i]->log.Find(j);
                ASSERT_NE(leaderEntry, nullptr);
                ASSERT_NE(entry, nullptr);
                EXPECT_EQ(entry->state, leaderEntry->state);
                EXPECT_TRUE(entry->state == LOG_STATE_RECEIVED || entry->state == LOG_STATE_NOOP);
                if (entry->state == LOG_STATE_RECEIVED) {
                    EXPECT_EQ(leaderEntry->request.op(), entry->request.op());
                } else {
                    committedGaps.insert(j);
                }
            }
        }
    }
    EXPECT_EQ(committedGaps.size(), 2);
    EXPECT_EQ(NUM_CLIENTS*NUM_PACKETS, numUpcalls);
    for (auto client : clients) {
        delete client.client;
    }
}
