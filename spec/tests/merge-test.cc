// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * merge-test.cc:
 *   test cases for Speculative Paxos log merging
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

#include "lib/configuration.h"
#include "common/log.h"
#include "common/replica.h"
#include "spec/replica.h"
#include "spec/tests/merge-test-case.pb.h"

#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <sstream>
#include <gtest/gtest.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <iostream>

#define TESTDIR "spec/tests/merge-tests"

using namespace specpaxos;
using namespace specpaxos::spec;
using namespace specpaxos::spec::proto;
using namespace specpaxos::spec::test;
using namespace google::protobuf;

class LogMergeTest : public testing::Test
{
protected:
    // We don't really need a real replica setup, but we need to have
    // a replica object initialized with an appropriate configuration
    // to call the merge function. Similarly, we're not even going to
    // use the transport, but the replica needs it.
    SpecReplica *replica;
    AppReplica *app;
    SimulatedTransport *transport;
    Configuration *config;

    virtual void SetUp() {
        map<int, vector<ReplicaAddress> > replicaAddrs =
        {
            {0, {
                    { "localhost", "12345" },
                    { "localhost", "12346" },
                    { "localhost", "12347" }
                }
            }
        };
        config = new Configuration(1, 3, 1, replicaAddrs);
        transport = new SimulatedTransport();
        app = new AppReplica();
        replica = new SpecReplica(*config, 0, true, transport, app);
    }

    virtual void TearDown() {
        delete replica;
        delete transport;
        delete config;
        delete app;
    }

    virtual void RunMergeTest(string path) {
        MergeTestCase tc;
        std::fstream input(path);
        io::IstreamInputStream inputStr(&input);

        ASSERT_TRUE(TextFormat::Parse(&inputStr, &tc));

        // Convert logs to DoViewChange messages
        std::map<int, DoViewChangeMessage> dvcs;
        for (auto x : tc.log()) {
            string hash = Log::EMPTY_HASH;
            int i = x.replicaidx();
            opnum_t lastCommitted = 0;
            opnum_t lastSpeculative = 0;
            dvcs[i].set_view(x.view());
            dvcs[i].set_lastnormalview(x.lastnormalview());
            dvcs[i].set_replicaidx(i);
            for (auto e: x.entries()) {
                // Parse clientid/clientreqid out of op info and build
                // a Request
                Request r;
                r.set_op(e.id());
                char buf[e.id().size()+1];
                strcpy(buf, e.id().c_str());
                char *cid = strtok(buf, "/");
                char *crid = strtok(NULL, "");
                r.set_clientid(strtol(cid, NULL, 0));
                r.set_clientreqid(strtol(crid, NULL, 0));

                LogEntryState state = e.spec() ? LOG_STATE_SPECULATIVE : LOG_STATE_COMMITTED;

                // Build and hash log entry
                LogEntry le(viewstamp_t(e.view(), e.opnum()),
                            state, r, "");
                le.hash = Log::ComputeHash(hash, le);
                hash = le.hash;

                auto n = dvcs[i].add_entries();
                n->set_view(e.view());
                n->set_opnum(e.opnum());
                *(n->mutable_request()) = r;
                n->set_state(state);
                n->set_hash(hash);

                // Keep track of lastSpeculative and lastCommitted
                // (and make sure the entries are in order)
                if (e.spec()) {
                    ASSERT_GT(e.opnum(), lastSpeculative);
                    lastSpeculative = e.opnum();
                } else {
                    ASSERT_GT(e.opnum(), lastCommitted);
                    lastCommitted = e.opnum();
                }
            }
            dvcs[i].set_lastspeculative(lastSpeculative);
            dvcs[i].set_lastcommitted(lastCommitted);

            // Make sure we've set all required fields
            ASSERT_TRUE(dvcs[i].IsInitialized());
        }

        // Do the merge
        vector<LogEntry> out;
        replica->MergeLogs(tc.newview(), 0, dvcs, out);

        // Check it against the expected result
        ASSERT_EQ(out.size(), tc.expected_size());
        for (opnum_t i = 1; i < out.size(); i++) {
            LogEntry oe = out[i-1];
            auto ee = tc.expected(i-1);

            ASSERT_EQ(ee.view(), oe.viewstamp.view);
            ASSERT_EQ(ee.opnum(), oe.viewstamp.opnum);
            if (ee.spec()) {
                ASSERT_EQ(oe.state, LOG_STATE_SPECULATIVE);
            } else {
                ASSERT_EQ(oe.state, LOG_STATE_COMMITTED);
            }
            ASSERT_EQ(ee.id(), oe.request.op());
        }
    }
};

#define MERGE_TEST(name)                        \
    TEST_F(LogMergeTest, name)                  \
    {                                           \
        RunMergeTest(TESTDIR "/" #name);        \
    }

MERGE_TEST(AllCommitted);
MERGE_TEST(SpeculativeQuorum);
MERGE_TEST(SpeculativeNoQuorum);
MERGE_TEST(Conflict)
MERGE_TEST(Divergence)
MERGE_TEST(EmptyLogs);
MERGE_TEST(OneEmpty);
MERGE_TEST(Stress);
