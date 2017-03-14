// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fastpaxos/replica.h:
 *   Fast Paxos protocol
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

#ifndef _FASTPAXOS_REPLICA_H_
#define _FASTPAXOS_REPLICA_H_

#include "lib/configuration.h"
#include "common/log.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "fastpaxos/fastpaxos-proto.pb.h"

#include <map>
#include <memory>
#include <list>

namespace specpaxos {
namespace fastpaxos {

class FastPaxosReplica : public Replica
{
public:
    FastPaxosReplica(Configuration config, int myIdx, bool initialize,
                     Transport *transport, AppReplica *app);
    ~FastPaxosReplica();

    void ReceiveMessage(const TransportAddress &remote,
                        const string &type, const string &data,
                        void *meta_data) override;

private:
    view_t view;
    opnum_t lastCommitted;
    opnum_t lastFastPath;
    opnum_t lastSlowPath;
    view_t lastRequestStateTransferView;
    opnum_t lastRequestStateTransferOpnum;
    std::list<std::pair<TransportAddress *,
                        proto::PrepareMessage> > pendingPrepares;
    std::list<std::pair<TransportAddress *,
                        proto::PrepareOKMessage> > pendingPrepareOKs;
    proto::PrepareMessage lastPrepare;

    Log log;
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
        uint64_t lastReqId;
        bool replied;
        proto::ReplyMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;

    QuorumSet<viewstamp_t, proto::PrepareOKMessage> slowPrepareOKQuorum;
    QuorumSet<viewstamp_t, proto::PrepareOKMessage> fastPrepareOKQuorum;

    Timeout *stateTransferTimeout;
    Timeout *resendPrepareTimeout;

    bool AmLeader() const;
    void CommitUpTo(opnum_t upto);
    void SendPrepareOKs(opnum_t oldLastOp);
    void RequestStateTransfer();
    void EnterView(view_t newview);
    void UpdateClientTable(const Request &req);
    void ResendPrepare();

    void HandleRequest(const TransportAddress &remote,
                       const proto::RequestMessage &msg);
    void HandleUnloggedRequest(const TransportAddress &remote,
                               const proto::UnloggedRequestMessage &msg);

    void HandlePrepare(const TransportAddress &remote,
                       const proto::PrepareMessage &msg);
    void HandlePrepareOK(const TransportAddress &remote,
                         const proto::PrepareOKMessage &msg);
    void HandleCommit(const TransportAddress &remote,
                      const proto::CommitMessage &msg);
    void HandleRequestStateTransfer(const TransportAddress &remote,
                                    const proto::RequestStateTransferMessage &msg);
    void HandleStateTransfer(const TransportAddress &remote,
                             const proto::StateTransferMessage &msg);
};

} // namespace specpaxos::vr
} // namespace specpaxos

#endif  /* _FASTPAXOS_REPLICA_H_ */
