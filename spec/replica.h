// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * spec/replica.h:
 *   Speculative Paxos protocol
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

#ifndef _SPEC_REPLICA_H_
#define _SPEC_REPLICA_H_

#include "lib/configuration.h"
#include "lib/latency.h"
#include "common/log.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "spec/spec-proto.pb.h"

#include <map>
#include <memory>
#include <set>
#include <list>
#include <vector>

class LogMergeTest;

namespace specpaxos {
namespace spec {

class SpecReplica : public Replica
{
public:
    SpecReplica(Configuration config, int myIdx, bool initialize,
                Transport *transport, AppReplica *app);
    ~SpecReplica();

    void ReceiveMessage(const TransportAddress &remote,
                        const string &type, const string &data,
                        void *meta_data);

public:                     // XXX public for unit testing
    Log log;
private:
    view_t view;
    opnum_t lastCommitted;
    opnum_t lastCommittedSent;
    opnum_t lastSpeculative;
    opnum_t pendingSync;
    opnum_t lastSync;
    view_t sentDoViewChange;
    view_t needFillDVC;
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
        uint64_t lastReqId;
        // We need the opnum to identify the correct entry in the
        // log. What we really want is the SpeculativeReplyMessage
        // corresponding to the last request, but we need to stuff
        // that in the log instead of keeping it here -- in order to
        // keep this up to date even if we roll back the log.
        opnum_t lastReqOpnum;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;
    std::list<std::pair<TransportAddress *,
                        proto::RequestMessage> > pendingRequests;

    QuorumSet<opnum_t, proto::SyncReplyMessage> syncReplyQuorum;
    QuorumSet<view_t, proto::StartViewChangeMessage> startViewChangeQuorum;
    QuorumSet<view_t, proto::DoViewChangeMessage> doViewChangeQuorum;
    QuorumSet<view_t, proto::InViewMessage> inViewQuorum;

    Timeout *syncTimeout;
    Timeout *failedSyncTimeout;
    Timeout *viewChangeTimeout;

    Latency_t reconciliationLatency;
    Latency_t mergeLatency;
    Latency_t requestLatency;

    bool AmLeader() const;
    void SendSync();
    void CommitUpTo(opnum_t upto);
    void RollbackTo(opnum_t backto);
    void UpdateClientTable(const Request &req,
                           LogEntry &entry,
                           const proto::SpeculativeReplyMessage &reply);
    void EnterView(view_t newview);
    void StartViewChange(view_t newview);
    void MergeLogs(view_t newView, opnum_t maxStart,
                   const std::map<int, proto::DoViewChangeMessage> &dvcs,
                   std::vector<LogEntry> &out);
    void InstallLog(const std::vector<LogEntry> &entries);
    void SendFillDVCGapMessage(int replicaIdx, view_t view);
    void NeedFillDVCGap(view_t view);
    void SendSyncReply(opnum_t opnum);

    void HandleRequest(const TransportAddress &remote,
                       const proto::RequestMessage &msg);
    void HandleUnloggedRequest(const TransportAddress &remote,
                               const proto::UnloggedRequestMessage &msg);
    void HandleSync(const TransportAddress &remote,
                    const proto::SyncMessage &msg);
    void HandleSyncReply(const TransportAddress &remote,
                         const proto::SyncReplyMessage &msg);
    void HandleRequestViewChange(const TransportAddress &remote,
                                 const proto::RequestViewChangeMessage &msg);
    void HandleStartViewChange(const TransportAddress &remote,
                               const proto::StartViewChangeMessage &msg);
    void HandleDoViewChange(const TransportAddress &remote,
                            const proto::DoViewChangeMessage &msg);
    void HandleStartView(const TransportAddress &remote,
                         const proto::StartViewMessage &msg);
    void HandleInView(const TransportAddress &remote,
                      const proto::InViewMessage &msg);
    void HandleFillLogGap(const TransportAddress &remote,
                          const proto::FillLogGapMessage &msg);
    void HandleFillDVCGap(const TransportAddress &remote,
                          const proto::FillDVCGapMessage &msg);

    friend class ::LogMergeTest;
};


} // namespace specpaxos::spec
} // namespace specpaxos

#endif  /* _SPEC_REPLICA_H_ */
