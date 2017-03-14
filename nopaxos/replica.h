// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * nopaxos/replica.h:
 *   Network-Ordered Paxos protocol replica implementation.
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *		  Jialin Li	   <lijl@cs.washington.edu>
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

#ifndef _NOPAXOS_REPLICA_H_
#define _NOPAXOS_REPLICA_H_

#include "lib/configuration.h"
#include "common/log.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "nopaxos/nopaxos-proto.pb.h"

#include <map>
#include <list>
#include <memory>
#include <set>

namespace specpaxos {
namespace nopaxos {

class NOPaxosReplica : public Replica
{
public:
    NOPaxosReplica(const Configuration &config, int myIdx, bool initialize,
                   Transport *transport, AppReplica *app);
    ~NOPaxosReplica();

    void ReceiveMessage(const TransportAddress &remote,
                        const string &type, const string &data,
                        void *meta_data) override;

public:
    Log log;

private:

    /* Replica states */
    view_t view;
    sessnum_t sessnum;
    opnum_t lastOp;
    msgnum_t nextMsgnum;

    view_t lastNormalView;
    sessnum_t lastNormalSessnum;
    // last committed operation. All operations upto lastCommittedOp
    // are guaranteed to be in the stable log. Only leader's
    // committed gaps can advance lastCommittedOp. Non-leader
    // replicas can only advance lastCommittedOp via synchronization
    // and view change.
    opnum_t lastCommittedOp;
    opnum_t lastExecutedOp;

    /* Client information */
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
        uint64_t lastReqId;
        proto::ReplyMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;

    /* Pending requests */
    std::list<proto::RequestMessage> pendingRequests;
    bool pendingRequestsSorted;

    /* Quorums */
    QuorumSet<opnum_t, proto::GapReplyMessage> gapReplyQuorum; // If none of the replicas received a message, the leader can immediately start gap agreement protocol
    QuorumSet<opnum_t, proto::GapCommitReplyMessage> gapCommitQuorum;
    QuorumSet<std::pair<sessnum_t, view_t>, proto::ViewChangeMessage> viewChangeQuorum;
    QuorumSet<std::pair<sessnum_t, view_t>, proto::StartViewReplyMessage> startViewQuorum;
    QuorumSet<opnum_t, proto::SyncPrepareReplyMessage> syncPrepareQuorum;

    /* Gaps */
    std::set<opnum_t> committedGaps;

    /* Timeouts */
    Timeout *gapRequestTimeout;
    const int GAP_REQUEST_TIMEOUT = 10;
    Timeout *gapCommitTimeout;
    const int GAP_COMMIT_TIMEOUT = 50;
    Timeout *viewChangeTimeout;
    const int VIEW_CHANGE_TIMEOUT = 100;
    Timeout *stateTransferTimeout;
    const int STATE_TRANSFER_TIMEOUT = 100;
    Timeout *startViewTimeout;
    const int START_VIEW_TIMEOUT = 100;
    Timeout *syncTimeout;
    const int SYNC_TIMEOUT = 1000;
    Timeout *leaderSyncHeardTimeout;
    const int LEADER_SYNC_HEARD_TIMEOUT = 2000;

    /* State transfer */
    opnum_t stateTransferOpBegin;
    opnum_t stateTransferOpEnd;
    uint32_t stateTransferReplicaIdx;
    std::function<void (void)> stateTransferCallback;

    /* View Change */
    sessnum_t startViewLastNormalSessnum;
    view_t startViewLastNormalView;
    opnum_t startViewLastOp;
    std::set<opnum_t> startViewCommittedGaps;

    /* Synchronization */
    opnum_t leaderLastSyncPreparePoint;
    std::set<opnum_t> leaderLastSyncCommittedGaps;
    std::set<opnum_t> receivedSyncPreparePoints;
    std::set<opnum_t> receivedSyncCommitPoints;
    const opnum_t MAX_SYNC_TRANSFER_OPS = 10;

    /* Message handlers */
    void HandleClientRequest(const TransportAddress &remote,
                             proto::RequestMessage &msg,
                             void *meta_data);
    void HandleUnloggedRequest(const TransportAddress &remote,
                               const proto::UnloggedRequestMessage &msg);
    void HandleGapRequest(const TransportAddress &remote,
                          const proto::GapRequestMessage &msg);
    void HandleGapReply(const TransportAddress &remote,
                        const proto::GapReplyMessage &msg);
    void HandleGapCommit(const TransportAddress &remote,
                         const proto::GapCommitMessage &msg);
    void HandleGapCommitReply(const TransportAddress &remote,
                              const proto::GapCommitReplyMessage &msg);
    void HandleStateTransferRequest(const TransportAddress &remote,
                                    const proto::StateTransferRequestMessage &msg);
    void HandleStateTransferReply(const TransportAddress &remote,
                                  const proto::StateTransferReplyMessage &msg);
    void HandleViewChangeRequest(const TransportAddress &remote,
                                 const proto::ViewChangeRequestMessage &msg);
    void HandleViewChange(const TransportAddress &remote,
                          const proto::ViewChangeMessage &msg);
    void HandleStartView(const TransportAddress &remote,
                         const proto::StartViewMessage &msg);
    void HandleStartViewReply(const TransportAddress &remote,
                              const proto::StartViewReplyMessage &msg);
    void HandleSyncPrepare(const TransportAddress &remote,
                           const proto::SyncPrepareMessage &msg);
    void HandleSyncPrepareReply(const TransportAddress &remote,
                                const proto::SyncPrepareReplyMessage &msg);
    void HandleSyncCommit(const TransportAddress &remote,
                          const proto::SyncCommitMessage &msg);
    void HandleSyncPrepareRequest(const TransportAddress &remote,
                                  const proto::SyncPrepareRequestMessage &msg);

    // Returns true if the request is processed/ignored.
    // false if the request should be processed later (pending)
    bool TryProcessClientRequest(const proto::RequestMessage &msg);
    void ProcessNextOperation(const specpaxos::Request &request,
                              viewstamp_t vs,
                              LogEntryState state);
    void ExecuteUptoOp(opnum_t opnum);
    void CommitUptoOp(opnum_t opnum);
    void UpdateClientTable(const Request &req,
                           const proto::ReplyMessage &reply);
    void ProcessPendingRequests();

    void StartGapAgreement();
    void StartViewChange(sessnum_t newsessnum, view_t newview);
    void EnterView(sessnum_t newsessnum, view_t newview);
    void ClearTimeoutAndQuorums();
    void InstallCommittedGaps(const ::google::protobuf::RepeatedField< ::google::protobuf::uint64 > &committedgaps);
    void ProcessSyncPrepare(opnum_t syncpoint);

    void SendGapRequest();
    void SendGapCommit();
    void SendViewChange();
    void SendStateTransferRequest();
    void SendStartView();
    void SendStartViewReply();
    void SendSyncPrepare();
    void SendSyncCommit();

    inline bool AmLeader() const;
    inline void AddPendingRequest(const proto::RequestMessage &msg);
    inline bool CheckViewNumAndStatus(sessnum_t sessnum, view_t view);
    inline void RewindLogToOpnum(opnum_t opnum);
};

} // namespace specpaxos::nopaxos
} // namespace specpaxos

#endif /* _NOPAXOS_REPLICA_H_ */
