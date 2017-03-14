// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * nopaxos/replica.cc:
 *   NOPaxos Replication protocol replica implementation.
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

#include "nopaxos/replica.h"
#include "nopaxos/nopaxos-proto.pb.h"

#include "lib/message.h"
#include "lib/assert.h"

#define RDebug(fmt, ...) Debug("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)

namespace specpaxos {
namespace nopaxos {

using namespace proto;

/* Comparator for sorting pending requests */
bool ComparePendingRequests(const RequestMessage &first,
                            const RequestMessage &second) {
    if (first.sessnum() != second.sessnum()) {
        return first.sessnum() < second.sessnum();
    }
    return first.msgnum() < second.msgnum();
}

NOPaxosReplica::NOPaxosReplica(const Configuration &config, int myIdx, bool initialize,
                               Transport *transport, AppReplica *app)
: Replica(config, 0, myIdx, initialize, transport, app),
    log(false),
    gapReplyQuorum(config.n-1),
    gapCommitQuorum(config.QuorumSize()-1),
    viewChangeQuorum(config.QuorumSize()-1),
    startViewQuorum(config.QuorumSize()-1),
    syncPrepareQuorum(config.QuorumSize()-1)
{
    this->status = STATUS_NORMAL;
    this->sessnum = 0;
    this->view = 0;
    this->lastOp = 0;
    this->nextMsgnum = 1;

    this->lastNormalView = 0;
    this->lastNormalSessnum = 0;
    this->lastCommittedOp = 0;
    this->lastExecutedOp = 0;

    this->pendingRequestsSorted = false;

    this->stateTransferOpBegin = 0;
    this->stateTransferOpEnd = 0;
    this->stateTransferReplicaIdx = 0;

    this->startViewLastNormalSessnum = 0;
    this->startViewLastNormalView = 0;
    this->startViewLastOp = 0;

    this->leaderLastSyncPreparePoint = 0;

    this->gapRequestTimeout = new Timeout(transport,
                                          GAP_REQUEST_TIMEOUT,
                                          [this, myIdx]() {
                                              RWarning("Gap request timed out!");
                                              if (AmLeader()) {
                                                  StartGapAgreement();
                                              } else {
                                                  SendGapRequest();
                                              }
                                          });
    this->gapCommitTimeout = new Timeout(transport,
                                         GAP_COMMIT_TIMEOUT,
                                         [this, myIdx]() {
                                             RWarning("Gap agreement timed out!");
                                             SendGapCommit();
                                         });
    this->viewChangeTimeout = new Timeout(transport,
                                          VIEW_CHANGE_TIMEOUT,
                                          [this,myIdx]() {
                                              RWarning("View change timed out!");
                                              SendViewChange();
                                          });
    this->stateTransferTimeout = new Timeout(transport,
                                             STATE_TRANSFER_TIMEOUT,
                                             [this, myIdx]() {
                                                 RWarning("State transfer timed out!");
                                                 SendStateTransferRequest();
                                             });
    this->startViewTimeout = new Timeout(transport,
                                         START_VIEW_TIMEOUT,
                                         [this, myIdx]() {
                                             RWarning("Start view timed out!");
                                             SendStartView();
                                         });
    this->syncTimeout = new Timeout(transport,
                                    SYNC_TIMEOUT,
                                    [this, myIdx]() {
                                        RDebug("Start Synchronization");
                                        SendSyncPrepare();
                                    });
    this->leaderSyncHeardTimeout = new Timeout(transport,
                                               LEADER_SYNC_HEARD_TIMEOUT,
                                               [this, myIdx]() {
                                                   RWarning("Starting view change; haven't received SyncPrepare from the leader");
                                                   StartViewChange(this->sessnum, this->view + 1);
                                               });
    if (AmLeader()) {
        this->syncTimeout->Start();
    } else {
        this->leaderSyncHeardTimeout->Start();
    }
}

NOPaxosReplica::~NOPaxosReplica()
{
    delete gapRequestTimeout;
    delete gapCommitTimeout;
    delete viewChangeTimeout;
    delete stateTransferTimeout;
    delete startViewTimeout;
    delete syncTimeout;
    delete leaderSyncHeardTimeout;
}


void
NOPaxosReplica::ReceiveMessage(const TransportAddress &remote,
                               const string &type, const string &data,
                               void *meta_data)
{
    static RequestMessage clientRequest;
    static UnloggedRequestMessage unloggedRequest;
    static GapRequestMessage gapRequest;
    static GapReplyMessage gapReply;
    static GapCommitMessage gapCommit;
    static GapCommitReplyMessage gapCommitReply;
    static StateTransferRequestMessage stateTransferRequest;
    static StateTransferReplyMessage stateTransferReply;
    static ViewChangeRequestMessage viewChangeRequest;
    static ViewChangeMessage viewChange;
    static StartViewMessage startView;
    static StartViewReplyMessage startViewReply;
    static SyncPrepareMessage syncPrepare;
    static SyncPrepareReplyMessage syncPrepareReply;
    static SyncCommitMessage syncCommit;

    if (type == clientRequest.GetTypeName()) {
        clientRequest.ParseFromString(data);
        HandleClientRequest(remote, clientRequest, meta_data);
    }
    else if (type == unloggedRequest.GetTypeName()) {
        unloggedRequest.ParseFromString(data);
        HandleUnloggedRequest(remote, unloggedRequest);
    }
    else if (type == gapRequest.GetTypeName()) {
        gapRequest.ParseFromString(data);
        HandleGapRequest(remote, gapRequest);
    }
    else if (type == gapReply.GetTypeName()) {
        gapReply.ParseFromString(data);
        HandleGapReply(remote, gapReply);
    }
    else if (type == gapCommit.GetTypeName()) {
        gapCommit.ParseFromString(data);
        HandleGapCommit(remote, gapCommit);
    }
    else if (type == gapCommitReply.GetTypeName()) {
        gapCommitReply.ParseFromString(data);
        HandleGapCommitReply(remote, gapCommitReply);
    }
    else if (type == stateTransferRequest.GetTypeName()) {
        stateTransferRequest.ParseFromString(data);
        HandleStateTransferRequest(remote, stateTransferRequest);
    }
    else if (type == stateTransferReply.GetTypeName()) {
        stateTransferReply.ParseFromString(data);
        HandleStateTransferReply(remote, stateTransferReply);
    }
    else if (type == viewChangeRequest.GetTypeName()) {
        viewChangeRequest.ParseFromString(data);
        HandleViewChangeRequest(remote, viewChangeRequest);
    }
    else if (type == viewChange.GetTypeName()) {
        viewChange.ParseFromString(data);
        HandleViewChange(remote, viewChange);
    }
    else if (type == startView.GetTypeName()) {
        startView.ParseFromString(data);
        HandleStartView(remote, startView);
    }
    else if (type == startViewReply.GetTypeName()) {
        startViewReply.ParseFromString(data);
        HandleStartViewReply(remote, startViewReply);
    }
    else if (type == syncPrepare.GetTypeName()) {
        syncPrepare.ParseFromString(data);
        HandleSyncPrepare(remote, syncPrepare);
    }
    else if (type == syncPrepareReply.GetTypeName()) {
        syncPrepareReply.ParseFromString(data);
        HandleSyncPrepareReply(remote, syncPrepareReply);
    }
    else if (type == syncCommit.GetTypeName()) {
        syncCommit.ParseFromString(data);
        HandleSyncCommit(remote, syncCommit);
    }
    else {
        Panic("Received unexpected message type in NOPaxos proto: %s",
              type.c_str());
    }

    ProcessPendingRequests();
}

void
NOPaxosReplica::HandleClientRequest(const TransportAddress &remote,
                                    RequestMessage &msg,
                                    void *meta_data)
{
    // Save client's address if not exist. Assume client addresses
    // never change.
    if (this->clientAddresses.find(msg.req().clientid()) == this->clientAddresses.end()) {
        this->clientAddresses.insert(std::pair<uint64_t, std::unique_ptr<TransportAddress> >(msg.req().clientid(), std::unique_ptr<TransportAddress>(remote.clone())));
    }

    if (meta_data != NULL) {
        // meta_data contains ordered multicast timestamp
        multistamp_t *ordered = (multistamp_t*)meta_data;
        msg.set_sessnum(ordered->sessnum);
        msg.set_msgnum(ordered->seqnums[0]);
    } else {
        // Simulated transport directly write sessnum and
        // msgnum into RequestMessage
        if (msg.sessnum() == 0 && msg.msgnum() == 0) {
            Panic("Client request has no ordering timestamp");
        }
    }

    if (!TryProcessClientRequest(msg)) {
        AddPendingRequest(msg);
    }
}

void
NOPaxosReplica::HandleUnloggedRequest(const TransportAddress &remote,
                                      const UnloggedRequestMessage &msg)
{
    UnloggedReplyMessage reply;

    ExecuteUnlogged(msg.req(), reply);

    if (!(transport->SendMessage(this, remote, reply))) {
        RWarning("Failed to send unlogged reply message");
    }
}

void
NOPaxosReplica::HandleGapRequest(const TransportAddress &remote,
                                 const GapRequestMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.sessnum(), msg.view())) {
        return;
    }

    GapReplyMessage reply;
    reply.set_view(this->view);
    reply.set_sessnum(this->sessnum);
    reply.set_opnum(msg.opnum());
    reply.set_replicaidx(this->replicaIdx);

    LogEntry * entry = log.Find(msg.opnum());

    if (entry) {
        if (entry->state == LOG_STATE_RECEIVED) {
            *(reply.mutable_req()) = entry->request;
            reply.set_isfound(true);
            reply.set_isgap(false);
            RDebug("Replying log entry %lu to the gap requester", msg.opnum());
        } else if (entry->state == LOG_STATE_NOOP) {
            reply.set_isfound(true);
            reply.set_isgap(true);
            RDebug("Replying log entry (gap) op %lu to the gap requester", msg.opnum());
        } else {
            NOT_REACHABLE();
        }
    } else {
        // Only reply with "message not received" when we are
        // actually missing the message (i.e. when we are also
        // requesting GapRequest for this message).
        if (this->gapRequestTimeout->Active() && this->lastOp+1 == msg.opnum()) {
            reply.set_isfound(false);
            reply.set_isgap(false);
            RDebug("Replica also has not received log entry %lu", msg.opnum());
        } else {
            return;
        }
    }
    if (!(transport->SendMessage(this, remote, reply))) {
        RWarning("Failed to send GapReplyMessage");
    }
}

void
NOPaxosReplica::HandleGapReply(const TransportAddress &remote,
                               const GapReplyMessage &msg) {
    if (!CheckViewNumAndStatus(msg.sessnum(), msg.view())) {
        return;
    }

    // We can ignore old GapReplyMessage
    if (msg.opnum() == this->lastOp+1) {
        if (this->status == STATUS_GAP_COMMIT) {
            // Leader has already started the gap agreement
            // protocol, ignore GapReplyMessage.
            ASSERT(AmLeader());
            return;
        }

        if (msg.isfound()) {
            viewstamp_t v;
            v.sessnum = this->sessnum;
            v.view = this->view;
            v.opnum = this->lastOp+1;
            v.msgnum = this->nextMsgnum;

            ProcessNextOperation(msg.req(), v, msg.isgap() ? LOG_STATE_NOOP : LOG_STATE_RECEIVED);
        } else {
            if (AmLeader()) {
                // Optimization: if the leader received 'not found' from
                // all replicas, it can immediately start the gap agreement
                // protocol, no need to wait for the timeout.
                if (gapReplyQuorum.AddAndCheckForQuorum(this->lastOp+1, msg.replicaidx(), msg)) {
                    StartGapAgreement();
                }
            }
        }
    }
}

void
NOPaxosReplica::HandleGapCommit(const TransportAddress &remote,
                                const GapCommitMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.sessnum(), msg.view())) {
        return;
    }

    RDebug("Received gap commit opnum %lu", msg.opnum());
    ASSERT(!AmLeader());

    // Do not reply to leader if have not received all
    // messages before the gap.
    if (this->lastOp + 1 < msg.opnum()) {
        this->committedGaps.insert(msg.opnum());
        return;
    }

    // If already received the corresponding client request
    // message, overwrite that log entry with NOOP. This
    // also handles duplicate GapCommitMessage: re-commit
    // a gap in the log is not a problem, and committedGaps
    // is a set, so Okay to insert multiple times.
    if (this->lastOp >= msg.opnum()) {
        this->committedGaps.insert(msg.opnum());
        this->log.SetStatus(msg.opnum(), LOG_STATE_NOOP);
    } else {
        // The committing gap is replica's next expected
        // message.
        ASSERT(this->lastOp + 1 == msg.opnum());
        Request noopRequest;
        viewstamp_t vs(this->view,
                       this->lastOp + 1,
                       this->sessnum,
                       this->nextMsgnum);
        ProcessNextOperation(noopRequest, vs, LOG_STATE_NOOP);
    }

    GapCommitReplyMessage gapCommitReplyMessage;
    gapCommitReplyMessage.set_sessnum(this->sessnum);
    gapCommitReplyMessage.set_view(this->view);
    gapCommitReplyMessage.set_opnum(msg.opnum());
    gapCommitReplyMessage.set_replicaidx(this->replicaIdx);

    if (!transport->SendMessage(this, remote, gapCommitReplyMessage)) {
        RWarning("Failed to send GapCommitReplyMessage");
    }
}

void
NOPaxosReplica::HandleGapCommitReply(const TransportAddress &remote,
                                     const GapCommitReplyMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.sessnum(), msg.view())) {
        return;
    }

    if (this->status != STATUS_GAP_COMMIT || !AmLeader()) {
        return;
    }

    RDebug("Received GapCommitReply opnum %lu from %u", msg.opnum(), msg.replicaidx());
    // Only consider GapCommitReply for the last operation:
    // we have already added NOOP to the log, waiting for a
    // quorum of replies for that NOOP operation.
    if (msg.opnum() == this->lastOp) {
        if (gapCommitQuorum.AddAndCheckForQuorum(msg.opnum(),
                                                 msg.replicaidx(),
                                                 msg)) {
            // We have a quorum, safe to continue processing
            // client requests.
            this->status = STATUS_NORMAL;
            gapCommitQuorum.Clear();
            gapCommitTimeout->Stop();
        }
    }
}

void
NOPaxosReplica::HandleStateTransferRequest(const TransportAddress &remote,
                                           const StateTransferRequestMessage &msg)
{
    // Only respond to state transfer request if in the
    // same view. If the requester is in a lower view, it
    // should first ask all the other replicas for the
    // latest view, update its viewid and then send
    // state transfer request. (XXX possibly a
    // LatestViewRequestMessage)
    // Can respond in STATUS_VIEW_CHANGE
    if (msg.sessnum() != this->sessnum || msg.view() != this->view) {
        return;
    }

    if (msg.begin() > this->lastOp || msg.end() > this->lastOp + 1) {
        return;
    }

    StateTransferReplyMessage reply;
    reply.set_view(this->view);
    reply.set_sessnum(this->sessnum);
    reply.set_begin(msg.begin());
    reply.set_end(msg.end());

    this->log.Dump(msg.begin(), msg.end(), reply.mutable_entries());

    RDebug("Sending StateTransfer from %lu to %lu",
           msg.begin(),
           msg.end());

    if (!(transport->SendMessage(this, remote, reply))) {
        RWarning("Failed to send StateTransferReplyMessage");
    }
}

void
NOPaxosReplica::HandleStateTransferReply(const TransportAddress &remote,
                                         const StateTransferReplyMessage &msg)
{
    if (msg.sessnum() != this->sessnum || msg.view() != this->view) {
        return;
    }

    // Ignore if already have all the log entries
    if (msg.end() - 1 <= this->lastOp) {
        return;
    }

    ASSERT(msg.entries(0).opnum() == msg.begin());
    ASSERT(msg.entries(msg.entries_size()-1).opnum() == msg.end()-1);

    RDebug("Installing StateTransfer from op %lu to %lu", this->lastOp+1, msg.end());

    for (int i = this->lastOp + 1 - msg.begin(); i < msg.entries().size(); i++) {
        auto &msgEntry = msg.entries(i);
        ASSERT(msgEntry.opnum() == this->lastOp+1);
        viewstamp_t vs(msgEntry.view(), msgEntry.opnum(), msgEntry.sessnum(), msgEntry.msgnum());

        // Okay to process the log entry with the state
        // in the received log. If we have already installed
        // a gap for this entry, ProcessNextOperation will
        // check committedGaps and install it as a NOOP correctly.
        ProcessNextOperation(msgEntry.request(), vs, static_cast<LogEntryState>(msgEntry.state()));
    }
    // ProcessNextOperation will check state transfer status, and
    // complete view change or synchronization.
}

void
NOPaxosReplica::HandleViewChangeRequest(const TransportAddress &remote,
                                        const ViewChangeRequestMessage &msg) {
    if (msg.sessnum() > this->sessnum || msg.view() > this->view) {
        StartViewChange(msg.sessnum(), msg.view());
    }
}

void
NOPaxosReplica::HandleViewChange(const TransportAddress &remote,
                                 const ViewChangeMessage &msg)
{
    if (msg.sessnum() > this->sessnum || msg.view() > this->view) {
        StartViewChange(msg.sessnum(), msg.view());
    }

    if (msg.sessnum() < this->sessnum || msg.view() < this->view) {
        return;
    }

    ASSERT(msg.sessnum() == this->sessnum &&
           msg.view() == this->view);

    ASSERT(AmLeader());

    if (this->status != STATUS_VIEW_CHANGE) {
        // Already entered the new view, potentially the
        // replica sending the ViewChange did not receive
        // the StartViewMessage. Resend StartViewMessage.
        // (but do not set the timer)
        StartViewMessage startViewMessage;
        startViewMessage.set_sessnum(this->sessnum);
        startViewMessage.set_view(this->view);
        startViewMessage.set_lastnormalsessnum(this->startViewLastNormalSessnum);
        startViewMessage.set_lastnormalview(this->startViewLastNormalView);
        startViewMessage.set_lastop(this->startViewLastOp);
        for (opnum_t gap : this->startViewCommittedGaps) {
            startViewMessage.add_committedgaps(gap);
        }

        if (!this->transport->SendMessage(this, remote, startViewMessage)) {
            RWarning("Failed to send StartViewMessage");
        }

        return;
    }

    if (auto msgs = this->viewChangeQuorum.AddAndCheckForQuorum(std::make_pair(msg.sessnum(), msg.view()),
                                                                msg.replicaidx(),
                                                                msg)) {
        RDebug("Leader doing ViewChange for sessnum %lu view %lu", msg.sessnum(), msg.view());
        // Merge logs
        sessnum_t latestNormalSessnum = this->lastNormalSessnum;
        view_t latestNormalView = this->lastNormalView;
        opnum_t latestOp = this->lastOp;
        // latestOpReplicaIdx = -1 indicates the leader has
        // the complete merged log, and does not need to do
        // state transfer.
        int latestOpReplicaIdx = -1;

        for (auto kv : *msgs) {
            // Find latest sessnum and view
            ViewChangeMessage &x = kv.second;
            if (x.lastnormalsessnum() > latestNormalSessnum) {
                latestNormalSessnum = x.lastnormalsessnum();
                latestOp = 0;
            }
            if (x.lastnormalview() > latestNormalView) {
                latestNormalView = x.lastnormalview();
                latestOp = 0;
            }
        }

        // We merge logs by adding log entries to this replica's
        // local log. If the replica has an older view ID in which
        // it had status NORMAL, it deletes all log entries after
        // the last commit point and will request through state transfer.
        // It also removes all committed gaps: ViewChangeMessages
        // contain all committed gaps, and we will add them accordingly.
        if (this->lastNormalSessnum < latestNormalSessnum ||
            this->lastNormalView < latestNormalView) {
            RewindLogToOpnum(this->lastCommittedOp);
            this->committedGaps.clear();
        }

        for (auto kv : *msgs) {
            // Find the last operation in the latest view ID,
            // and merge committed gaps. Only need to consider
            // committed gaps from logs in the latest view (since
            // view change guarantee they will contain all committed
            // gaps from previous views). We could potentially include
            // all operations in the latest session (not necessary in
            // the same leader view), however, some operations may be
            // in different log positions, so more complicated to perform
            // merging. Here we use the simple and safe option: only consider
            // logs from the latest session and leader view.
            ViewChangeMessage &x = kv.second;
            ASSERT(x.lastnormalsessnum() <= latestNormalSessnum);
            ASSERT(x.lastnormalview() <= latestNormalView);
            if (x.lastnormalsessnum() == latestNormalSessnum &&
                x.lastnormalview() == latestNormalView) {
                InstallCommittedGaps(x.committedgaps());

                if (x.lastop() > latestOp) {
                    latestOp = x.lastop();
                    latestOpReplicaIdx = x.replicaidx();
                }
            }
        }

        // All gaps should be within log boundary
        auto lastGap = this->committedGaps.rbegin();
        if (lastGap != this->committedGaps.rend()) {
            ASSERT(*lastGap <= latestOp);
        }

        // At this point, we have determined the merged log
        // for the StartViewMessage. Record them here for
        // SendStartView (and potential retries). Note that
        // we may not have the entrie merged log yet, but we
        // only send StartView after we received the state
        // transfer.
        this->startViewLastNormalSessnum = latestNormalSessnum;
        this->startViewLastNormalView = latestNormalView;
        this->startViewLastOp = latestOp;
        this->startViewCommittedGaps = this->committedGaps;

        // Request (if any) missing operations from the
        // replica with the latest operation (in the highest
        // normal view). Assume we can contact that replica.
        // If that replica has failed, we need to contact
        // another set of f replicas to either find those
        // missing operations, or make sure the a majority
        // of replicas do not have those operations (XXX
        // this part is not implemented yet). This is essentially
        // an optimization that reduces the size of ViewChangeMessage.
        if (latestOpReplicaIdx != -1) {
            ASSERT(latestOp > this->lastOp);
            this->stateTransferOpBegin = this->lastOp+1;
            this->stateTransferOpEnd = latestOp+1;
            this->stateTransferReplicaIdx = latestOpReplicaIdx;
            this->stateTransferCallback = [this]() {
                EnterView(this->sessnum, this->view);
            };

            SendStateTransferRequest();
        } else {
            ASSERT(latestOp == this->lastOp);
            // Our log is the merged log, can enter new view.
            EnterView(this->sessnum, this->view);
        }
    }
}

void
NOPaxosReplica::HandleStartView(const TransportAddress &remote,
                                const StartViewMessage &msg)
{
    if (msg.sessnum() < this->sessnum || msg.view() < this->view) {
        // Ignore STARTVIEW from an older view
        return;
    }

    if ((msg.sessnum() == this->sessnum)
        && (msg.view() == view)
        && (status != STATUS_VIEW_CHANGE)) {
        // We have already started the new view. Acknowledge
        // the leader again.
        SendStartViewReply();
        return;
    }

    RDebug("Received StartView for sessnum %lu view %lu", msg.sessnum(), msg.view());

    ASSERT(configuration.GetLeaderIndex(msg.view()) != this->replicaIdx);

    // If the replica is lagging behind in view ID, it may
    // have the wrong operations and/or wrong gaps. Rewind
    // the log to the last commit point.
    if (this->lastNormalSessnum < msg.lastnormalsessnum() ||
        this->lastNormalView < msg.lastnormalview()) {
        RewindLogToOpnum(this->lastCommittedOp);
    } else {
        // The replica was in the latest view before view
        // change. If the replica has more operations than
        // the merged log, remove those operations. A
        // potential optimization is to not remove those
        // operations if only the leader view is changed
        // (not session change), but this simplified version
        // is always correct.
        ASSERT(this->lastNormalSessnum == msg.lastnormalsessnum());
        ASSERT(this->lastNormalView == msg.lastnormalview());
        if (this->lastOp > msg.lastop()) {
            RewindLogToOpnum(msg.lastop());
        }
    }

    // Install all the committed gaps. Since each gap requires
    // a quorum to commit and the merged log is also taken from
    // a quorum, replica will not have committed gaps that are
    // not in the merged log. Therefore, safe just to install
    // gaps from the message. The only exception is the last
    // committed gap. It's possible that only a minority of replicas
    // received the GapCommit and did not participate in the
    // view change (this won't happen to previous committed gaps
    // because the leader will not proceed until getting
    // quorum replies). In that case, need to ask for that
    // operation if it is in the merged log. Here we use the simple
    // implementation that simply rewind the log to before the gap. Note
    // that we are relying on std::set to be ordered, and we add
    // them into the message in order too.
    if (this->lastNormalSessnum == msg.lastnormalsessnum() &&
        this->lastNormalView == msg.lastnormalview()) {
        if (!this->committedGaps.empty()) {
            opnum_t lastGapOpnum = *this->committedGaps.rbegin();
            if (msg.committedgaps().size() > 0 &&
                msg.committedgaps().Get(msg.committedgaps().size()-1) < lastGapOpnum) {
                RewindLogToOpnum(lastGapOpnum-1);
            }
        }
    }
    this->committedGaps.clear();
    InstallCommittedGaps(msg.committedgaps());

    if (this->lastOp < msg.lastop()) {
        // Need to request missing entries from the leader
        // make sure we are in view change status
        this->sessnum = msg.sessnum();
        this->view = msg.view();
        this->status = STATUS_VIEW_CHANGE;
        ClearTimeoutAndQuorums();

        this->stateTransferOpBegin = this->lastOp+1;
        this->stateTransferOpEnd = msg.lastop()+1;
        this->stateTransferReplicaIdx = configuration.GetLeaderIndex(msg.view());
        this->stateTransferCallback = [this]() {
            EnterView(this->sessnum, this->view);
        };

        SendStateTransferRequest();
    } else {
        // We have all the merged log, can enter
        // the new view.
        EnterView(msg.sessnum(), msg.view());
    }
}

void
NOPaxosReplica::HandleStartViewReply(const TransportAddress &remote,
                                     const StartViewReplyMessage &msg)
{
    if (msg.sessnum() != this->sessnum ||
        msg.view() != this->view) {
        // Ignore start view reply from a different
        // view ID. Reply should never contain a
        // view ID larger than the current view ID.
        return;
    }

    ASSERT(this->status == STATUS_NORMAL);
    ASSERT(AmLeader());

    // If we haven't heard back from a quorum of replicas
    // acknowledging they have started the new view. A quorum
    // guarantees the log upto where the view starts is
    // committed.
    if (this->startViewTimeout->Active()) {
        if (this->startViewQuorum.AddAndCheckForQuorum(std::make_pair(msg.sessnum(), msg.view()),
                                                       msg.replicaidx(),
                                                       msg)) {
            // If we have not committed operations upto where
            // the view starts, safe to commit them now.
            CommitUptoOp(this->startViewLastOp);

            // And we can stop sending StartViewMessage.
            // Ideally we should keep sending until all
            // replicas enter the new view, but synchronization
            // should bring all replicas up to the latest view.
            this->startViewTimeout->Stop();
            this->startViewQuorum.Clear();
        }
    }
}

void
NOPaxosReplica::HandleSyncPrepare(const TransportAddress &remote,
                                  const proto::SyncPrepareMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.sessnum(), msg.view())) {
        return;
    }

    this->leaderSyncHeardTimeout->Reset();

    // If we have already committed operations beyond the
    // sync point, can ignore the message.
    if (this->lastCommittedOp >= msg.lastop()) {
        return;
    }

    ASSERT(this->sessnum == msg.sessnum() && this->view == msg.view());
    ASSERT(!AmLeader());
    this->receivedSyncPreparePoints.insert(msg.lastop());

    // We are in the same view ID as the leader, so our
    // committed gaps is a subset of the leader's. Safe
    // to simply install them here (we won't have gaps
    // the leader doesn't know about).
    InstallCommittedGaps(msg.committedgaps());

    if (this->lastOp < msg.lastop()) {
        // Need to request missing entries from the leader.
        this->stateTransferOpBegin = this->lastOp+1;
        this->stateTransferOpEnd = msg.lastop()+1;
        this->stateTransferReplicaIdx = configuration.GetLeaderIndex(msg.view());
        opnum_t syncpoint = msg.lastop();
        this->stateTransferCallback = [this, syncpoint] {
            ProcessSyncPrepare(syncpoint);
        };

        // If replica is lagging far behind, don't ask for
        // state transfer (this will create big messages).
        // Try processing existing requests first.
        if (msg.lastop() - this->lastOp <= MAX_SYNC_TRANSFER_OPS) {
            SendStateTransferRequest();
        }
    } else {
        ProcessSyncPrepare(msg.lastop());
    }
}

void
NOPaxosReplica::HandleSyncPrepareReply(const TransportAddress &remote,
                                       const proto::SyncPrepareReplyMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.sessnum(), msg.view())) {
        return;
    }

    ASSERT(AmLeader());

    // If we have already committed the operation, ignore
    // this reply.
    if (this->lastCommittedOp >= msg.syncpoint()) {
        return;
    }

    // Ignore sync prepare reply for a different sync point
    if (msg.syncpoint() != this->leaderLastSyncPreparePoint) {
        return;
    }

    // We need a quorum of sync prepare replies to commit.
    if (this->syncPrepareQuorum.AddAndCheckForQuorum(msg.syncpoint(),
                                                     msg.replicaidx(),
                                                     msg)) {
        CommitUptoOp(msg.syncpoint());
    }
}

void
NOPaxosReplica::HandleSyncCommit(const TransportAddress &remote,
                                 const proto::SyncCommitMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.sessnum(), msg.view())) {
        return;
    }

    ASSERT(!AmLeader());

    // If we have already committed operations beyond the
    // sync point, can ignore the message.
    if (this->lastCommittedOp >= msg.syncpoint()) {
        return;
    }

    ASSERT(this->sessnum == msg.sessnum() && this->view == msg.view());
    this->receivedSyncCommitPoints.insert(msg.syncpoint());

    if (this->receivedSyncPreparePoints.find(msg.syncpoint()) !=
        this->receivedSyncPreparePoints.end()) {
        // We have already received the corresponding
        // SyncPrepareMessage, so we are guaranteed to have
        // received the correct gaps and operations (potentially
        // still doing state transfer). Commit the operations
        // if we do have all the ops, otherwise state transfer
        // will handle the commit later.
        if (this->lastOp >= msg.syncpoint()) {
            CommitUptoOp(msg.syncpoint());
            this->receivedSyncPreparePoints.erase(msg.syncpoint());
            this->receivedSyncCommitPoints.erase(msg.syncpoint());
        }
    } else {
        // We will request the SyncPrepareMessage from the
        // leader.
        SyncPrepareRequestMessage syncPrepareRequestMessage;
        syncPrepareRequestMessage.set_sessnum(this->sessnum);
        syncPrepareRequestMessage.set_view(this->view);

        if (!this->transport->SendMessageToReplica(this,
                                                   this->configuration.GetLeaderIndex(this->view),
                                                   syncPrepareRequestMessage)) {
            RWarning("Failed to send SyncPrepareRequestMessage");
        }
    }
}

void
NOPaxosReplica::HandleSyncPrepareRequest(const TransportAddress &remote,
                                         const proto::SyncPrepareRequestMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.sessnum(), msg.view())) {
        return;
    }

    ASSERT(AmLeader());

    SyncPrepareMessage syncPrepareMessage;
    syncPrepareMessage.set_sessnum(this->sessnum);
    syncPrepareMessage.set_view(this->view);
    syncPrepareMessage.set_lastop(this->leaderLastSyncPreparePoint);
    for (opnum_t gap : this->leaderLastSyncCommittedGaps) {
        syncPrepareMessage.add_committedgaps(gap);
    }

    if (!this->transport->SendMessage(this, remote, syncPrepareMessage)) {
        RWarning("Failed to send SyncPrepareMessage");
    }

    // If we have already committed the sync point, also send
    // a SyncCommit to the requester.
    if (this->lastCommittedOp >= this->leaderLastSyncPreparePoint) {
        SyncCommitMessage syncCommitMessage;
        syncCommitMessage.set_sessnum(this->sessnum);
        syncCommitMessage.set_view(this->view);
        syncCommitMessage.set_syncpoint(this->leaderLastSyncPreparePoint);

        if (!this->transport->SendMessage(this, remote, syncCommitMessage)) {
            RWarning("Failed to send SyncCommitMessage");
        }
    }
}

bool
NOPaxosReplica::TryProcessClientRequest(const RequestMessage &msg)
{
    if (this->status != STATUS_NORMAL) {
        // Replica currently not processing any client
        // requests. Add it to pending requests and try
        // later.
        return false;
    }

    if (msg.sessnum() < this->sessnum) {
        return true;
    }

    if (msg.sessnum() > this->sessnum) {
        // new session
        StartViewChange(msg.sessnum(), this->view);
        // Try the request later when we move to
        // the new session.
        return false;
    }

    if (msg.msgnum() < this->nextMsgnum) {
        return true;
    } else if (msg.msgnum() > this->nextMsgnum) {
        // Detected message gap
        // None leader replicas ask the leader for the
        // missing message. Leader replica as an optimization
        // will ask other replicas for the missing message
        // before initiating a gap agreement protocol. Do
        // not ask again if we have already done so: timeout
        // will make sure we resend the request.
        if (!gapRequestTimeout->Active() &&
            !gapCommitTimeout->Active()) {
            SendGapRequest();
        }
        // Try the request later once we received all
        // previous requests.
        return false;
    } else {
        // Received the next message in the session
        viewstamp_t vs;
        vs.view = this->view;
        vs.opnum = this->lastOp+1;
        vs.sessnum = msg.sessnum();
        vs.msgnum = msg.msgnum();

        ProcessNextOperation(msg.req(), vs, LOG_STATE_RECEIVED);
        return true;
    }
}

void
NOPaxosReplica::ProcessNextOperation(const Request &request,
                                     viewstamp_t vs,
                                     LogEntryState state)
{
    ASSERT(vs.opnum == this->lastOp+1);

    this->lastOp = vs.opnum;
    this->nextMsgnum = vs.msgnum + 1;

    // This log entry has already been decided as
    // a gap (from view change/synchronization).
    if (this->committedGaps.find(vs.opnum) != this->committedGaps.end()) {
        state = LOG_STATE_NOOP;
    }

    this->log.Append(vs, request, state);

    // If we have sent a GapRequestMessage, we can cancel
    // the timeout. Either some other replica responds with the
    // message, or the replica receives the message from the
    // client. Note if the leader is in the gap agreement
    // protocol, it won't process any client request, and
    // will eventually commit the log slot as NOOP.
    if (this->gapRequestTimeout->Active()) {
        this->gapRequestTimeout->Stop();
        this->gapReplyQuorum.Clear();
    }

    if (state == LOG_STATE_NOOP) {
        this->committedGaps.insert(this->lastOp);
    } else if (state == LOG_STATE_RECEIVED) {
        // Only the leader execute the request. The
        // leader has to execute all previous operations
        // before execute this one.
        if (this->configuration.GetLeaderIndex(vs.view) == this->replicaIdx) {
            // This function will reply to client properly.
            ExecuteUptoOp(vs.opnum);
        } else {
            // Non-leader replica simply reply without execution.
            ReplyMessage reply;
            auto addr = this->clientAddresses.find(request.clientid());
            if (addr != this->clientAddresses.end()) {
                reply.set_clientreqid(request.clientreqid());
                reply.set_replicaidx(this->replicaIdx);
                reply.set_view(vs.view);
                reply.set_opnum(vs.opnum);
                reply.set_sessnum(vs.sessnum);

                if (!this->transport->SendMessage(this, *(addr->second), reply)) {
                    RWarning("Failed to send reply to client");
                }
            }
        }
    }

    if (vs.opnum == this->stateTransferOpEnd) {
        // We have completed the state transfer, can stop
        // the timeout now and complete the view change or
        // synchronization.
        this->stateTransferTimeout->Stop();
        this->stateTransferCallback();
    }

    // We may have installed committed gaps beyond the lastOp.
    // Can safely process them in log order now.
    if (this->committedGaps.find(this->lastOp+1) != this->committedGaps.end()) {
        vs.opnum = this->lastOp+1;
        vs.msgnum = this->nextMsgnum;
        ProcessNextOperation(request, vs, LOG_STATE_NOOP);
    }
}

void
NOPaxosReplica::ExecuteUptoOp(opnum_t opnum)
{
    if (opnum <= this->lastExecutedOp) {
        // Already executed
        return;
    }

    if (opnum > this->lastOp) {
        RPanic("Executing operation not received yet");
    }

    for (opnum_t op = this->lastExecutedOp + 1; op <= opnum; op++) {
        ASSERT(op <= this->lastOp);
        LogEntry *entry = this->log.Find(op);
        ASSERT(op == entry->viewstamp.opnum);

        // We only execute the operation if the
        // entry is not a gap. But always update
        // lastExecutedOp even for gaps.
        this->lastExecutedOp = op;
        if (entry->state == LOG_STATE_RECEIVED) {
            Request request = entry->request;
            ReplyMessage reply;

            // Check client table for duplicate requests.
            auto kv = this->clientTable.find(request.clientid());
            if (kv != this->clientTable.end()) {
                const ClientTableEntry &entry = kv->second;
                if (request.clientreqid() < entry.lastReqId) {
                    // Ignore stale request
                    continue;
                }
                if (request.clientreqid() == entry.lastReqId) {
                    // Duplicate request (potentially client
                    // retry). Send back the last reply.
                    reply.set_reply(entry.reply.reply());
                }
            }

            // Only execute if this is not a duplicate
            // of the last request (client table check
            // will otherwise fill the reply).
            if (!reply.has_reply()) {
                Execute(op, request, reply);
                UpdateClientTable(request, reply);
            }

            // Only reply back to client if the replica
            // is the leader of view this operation belongs
            // to. Otherwise, it has already responded to
            // the client during ProcessNextOperation.
            if (this->configuration.GetLeaderIndex(entry->viewstamp.view) == this->replicaIdx) {
                ASSERT(reply.has_reply());
                auto addr = this->clientAddresses.find(request.clientid());
                if (addr != this->clientAddresses.end()) {
                    reply.set_clientreqid(request.clientreqid());
                    reply.set_replicaidx(this->replicaIdx);
                    reply.set_view(entry->viewstamp.view);
                    reply.set_opnum(op);
                    reply.set_sessnum(entry->viewstamp.sessnum);

                    if (!this->transport->SendMessage(this, *(addr->second), reply)) {
                        RWarning("Failed to send reply to client");
                    }
                }
            }
        }
    }
}

void
NOPaxosReplica::CommitUptoOp(opnum_t opnum)
{
    // If already committed, ignore.
    if (opnum <= this->lastCommittedOp) {
        return;
    }

    if (opnum > this->lastOp) {
        RPanic("Committing operations not received yet");
    }

    this->lastCommittedOp = opnum;

    // Leader replica inform the other replicas
    // about the commit.
    if (AmLeader()) {
        SendSyncCommit();
    }

    // Safe to execute operations up to the
    // commit point (for both leader and
    // non-leader).
    ExecuteUptoOp(opnum);
}

void
NOPaxosReplica::UpdateClientTable(const Request &req,
                                  const proto::ReplyMessage &reply)
{
    ClientTableEntry &entry = clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId == req.clientreqid()) {
        // Duplicate request
        return;
    }

    entry.lastReqId = req.clientreqid();
    if (AmLeader()) {
        // Only leader needs to store the reply
        // replicas do not execute the request
        entry.reply = reply;
    }
}

void
NOPaxosReplica::ProcessPendingRequests()
{
    if (this->status != STATUS_NORMAL) {
        return;
    }

    // Try if we can process pending requests
    // sort the pending requests in increasing sessnum msgnum first (performance optimization)
    if (!this->pendingRequests.empty()) {
        if (!this->pendingRequestsSorted) {
            this->pendingRequests.sort(ComparePendingRequests);
            this->pendingRequestsSorted = true;
        }

        while (!this->pendingRequests.empty()) {
            RequestMessage request = this->pendingRequests.front();
            if (!TryProcessClientRequest(request)) {
                // request is still pending, since we have
                // already sorted the list, all subsequent
                // requests will also be pending. Safe to return.
                return;
            }
            // request is either processed, or is discarded
            pendingRequests.pop_front();
        }
    }
}

void
NOPaxosReplica::StartGapAgreement()
{
    ASSERT(AmLeader());
    this->gapReplyQuorum.Clear();
    this->gapRequestTimeout->Stop();

    RDebug("Initiating GapAgreement for op %lu", this->lastOp + 1);

    // Change status to GAPCOMMIT, subsequent client requests
    // won't be processed until gap is committed (request added
    // to the pending queue).
    this->status = STATUS_GAP_COMMIT;
    this->gapCommitQuorum.Clear();

    // Append NOOP to the log. Okay to do it before
    // getting a quorum of replies from other replicas,
    // since it (leader) will not process subsequent
    // client requests until it received a quorum of replies.
    Request noopRequest;
    viewstamp_t vs(this->view,
                   this->lastOp + 1,
                   this->sessnum,
                   this->nextMsgnum);
    ProcessNextOperation(noopRequest, vs, LOG_STATE_NOOP);

    SendGapCommit();
}

void
NOPaxosReplica::StartViewChange(sessnum_t newsessnum, view_t newview)
{
    ASSERT(newsessnum > this->sessnum || newview > this->view);
    // We never decrease view IDs, so take the maximum of current
    // view ID and new view ID.
    newsessnum = newsessnum > this->sessnum ? newsessnum : this->sessnum;
    newview = newview > this->view ? newview : this->view;

    RDebug("Starting view change sessnum %lu view %lu", newsessnum, newview);

    ClearTimeoutAndQuorums();

    this->sessnum = newsessnum;
    this->view = newview;
    if (newsessnum > this->log.LastViewstamp().sessnum) {
        this->nextMsgnum = 1;
    }
    this->status = STATUS_VIEW_CHANGE;

    // If we have committed gaps beyond the last log entry,
    // remove them now. It is safe because we have not
    // replied to the corresponding GapCommits, and since
    // gap agreement requires quorum of GapCommitReply,
    // at least one replica for each of those gaps (with
    // enough log entries) will participate in the view
    // change. Only exception is the last committed gap if
    // the leader has not received enough GapCommitReply and
    // do not participate in the view change. However, since the
    // leader have not proceeded yet, it is safe to ignore that
    // gap.
    while (true) {
        // We rely on std::set being ordered
        auto lastGap = this->committedGaps.rbegin();
        if (lastGap == this->committedGaps.rend()) {
            break;
        }
        if (*lastGap > this->lastOp) {
            this->committedGaps.erase(*lastGap);
        } else {
            // since set is ordered, previous gaps
            // are within log boundary.
            break;
        }
    }

    SendViewChange();
}

void
NOPaxosReplica::EnterView(sessnum_t newsessnum, view_t newview) {
    ASSERT(newsessnum >= this->sessnum);
    ASSERT(newview >= this->view);
    RNotice("Entering new sessnum %lu view %lu", newsessnum, newview);

    // Reset nextMsgnum if we are entering a new session.
    // Note that we use the log's last entry's session number,
    // not lastNormalSessnum here (the replica may not participate
    // in a recent session change). It is always correct to
    // reset the nextMsgnum when the next log position expects
    // a new session. If the session number does not change,
    // nextMsgnum is updated consistently during state transfer.
    if (newsessnum > this->log.LastViewstamp().sessnum) {
        this->nextMsgnum = 1;
    }
    this->sessnum = newsessnum;
    this->view = newview;
    this->lastNormalSessnum = newsessnum;
    this->lastNormalView = newview;
    this->status = STATUS_NORMAL;

    // Clear before calling SendStartView since it
    // will reset startViewTimeout.
    ClearTimeoutAndQuorums();

    // As an optimization, view change serves as
    // a synchronization as well. After entering
    // the new view, all replicas have the same
    // log and thus safe to commit all operations.
    // Leader has to wait for a quorum of StartView
    // replies to actually commit. Non-leader replicas
    // record the SyncPrepare point here.
    if (AmLeader()) {
        // New leader safe to execute all ops
        ExecuteUptoOp(this->lastOp);
        this->syncTimeout->Start();
        ASSERT(this->leaderLastSyncPreparePoint <= this->lastOp);
        this->leaderLastSyncPreparePoint = this->lastOp;
        this->leaderLastSyncCommittedGaps = this->committedGaps;
        SendStartView();
    } else {
        this->leaderSyncHeardTimeout->Start();
        this->receivedSyncPreparePoints.insert(this->lastOp);
        // Non-leader replicas acknowledge the leader.
        SendStartViewReply();
    }
}

void
NOPaxosReplica::ClearTimeoutAndQuorums()
{
    this->gapRequestTimeout->Stop();
    this->gapReplyQuorum.Clear();
    this->gapCommitTimeout->Stop();
    this->gapCommitQuorum.Clear();
    this->viewChangeTimeout->Stop();
    this->viewChangeQuorum.Clear();
    this->stateTransferTimeout->Stop();
    this->startViewTimeout->Stop();
    this->startViewQuorum.Clear();
    this->syncTimeout->Stop();
    this->syncPrepareQuorum.Clear();
    this->leaderSyncHeardTimeout->Stop();

    // Since we only accept Sync messages from the
    // current view, we can delete all SyncPrepare
    // and SyncCommit messages from the previous
    // views during view change.
    this->receivedSyncPreparePoints.clear();
    this->receivedSyncCommitPoints.clear();

    // Stop all ongoing state transfer
    this->stateTransferOpBegin = 0;
    this->stateTransferOpEnd = 0;
}

void
NOPaxosReplica::InstallCommittedGaps(const ::google::protobuf::RepeatedField< ::google::protobuf::uint64 > &committedgaps)
{
    for (int i = 0; i < committedgaps.size(); i++) {
        opnum_t gap = committedgaps.Get(i);
        if (this->committedGaps.find(gap) == this->committedGaps.end()) {
            this->committedGaps.insert(gap);
            // If this gap is the next operation we are expecting,
            // can advance log now.
            if (this->lastOp+1 == gap) {
                viewstamp_t vs = this->log.LastViewstamp();
                vs.opnum = this->lastOp+1;
                vs.msgnum = this->nextMsgnum;
                Request noopRequest;
                ProcessNextOperation(noopRequest, vs, LOG_STATE_NOOP);
            }
            // Only overwrite log entry to NOOP if we already have
            // that entry. If not, we will state transfer the missing
            // operations, and overwrite them to NOOP when we receive
            // them.
            if (this->lastOp >= gap && this->log.Find(gap)->state != LOG_STATE_NOOP) {
                ASSERT(this->lastCommittedOp < gap);
                if (this->lastExecutedOp >= gap) {
                    RPanic("Trying to overwrite operation that has already executed. However rollback is not implemented yet");
                }
                this->log.SetStatus(gap, LOG_STATE_NOOP);
            }
        }
    }
}

void
NOPaxosReplica::ProcessSyncPrepare(opnum_t syncpoint)
{
    ASSERT(!AmLeader());
    // We already have the complete log upto the sync point.
    // If we have already received a corresponding SyncCommit,
    // commit them now.
    if (this->receivedSyncCommitPoints.find(syncpoint) !=
        this->receivedSyncCommitPoints.end()) {
        CommitUptoOp(syncpoint);
        this->receivedSyncPreparePoints.erase(syncpoint);
        this->receivedSyncCommitPoints.erase(syncpoint);
    } else {
        // Otherwise, we acknowlege the leader we have received
        // the sync prepare message.
        SyncPrepareReplyMessage reply;
        reply.set_sessnum(this->sessnum);
        reply.set_view(this->view);
        reply.set_syncpoint(syncpoint);
        reply.set_replicaidx(this->replicaIdx);

        if (!this->transport->SendMessageToReplica(this,
                                                   configuration.GetLeaderIndex(this->view),
                                                   reply)) {
            RWarning("Failed to send SyncPrepareReplyMessage to leader");
        }
    }
}

void
NOPaxosReplica::SendGapRequest()
{
    GapRequestMessage gapRequestMessage;
    gapRequestMessage.set_view(this->view);
    gapRequestMessage.set_sessnum(this->sessnum);
    gapRequestMessage.set_opnum(this->lastOp+1);

    RDebug("Sending GapRequestMessage with opnum %lu", this->lastOp+1);
    if (!AmLeader()) {
        if (!this->transport->SendMessageToReplica(this,
                                                   this->configuration.GetLeaderIndex(this->view),
                                                   gapRequestMessage)) {
            RWarning("Failed to send GapRequestMessage to leader");
        }
    } else {
        if (!this->transport->SendMessageToAll(this,
                                               gapRequestMessage)) {
            RWarning("Failed to send GapRequestMessage to replicas");
        }
    }
    this->gapRequestTimeout->Reset();
}

void
NOPaxosReplica::SendGapCommit()
{
    ASSERT(AmLeader());
    ASSERT(this->status == STATUS_GAP_COMMIT);

    GapCommitMessage gapCommitMessage;
    gapCommitMessage.set_sessnum(this->sessnum);
    gapCommitMessage.set_view(this->view);
    // not lastOp+1 because we have already appended
    // NOOP to the log.
    gapCommitMessage.set_opnum(this->lastOp);

    if (!(this->transport->SendMessageToAll(this, gapCommitMessage))) {
        RWarning("Failed to send GapCommitMessage");
    }
    this->gapCommitTimeout->Reset();
}

void
NOPaxosReplica::SendViewChange()
{
    ViewChangeRequestMessage viewChangeRequestMessage;
    viewChangeRequestMessage.set_sessnum(this->sessnum);
    viewChangeRequestMessage.set_view(this->view);

    if (!transport->SendMessageToAll(this, viewChangeRequestMessage)) {
        RWarning("Failed to send ViewChangeRequestMessage to all replicas");
    }

    // Leader does not need to send viewchange message
    // to itself.
    if (!AmLeader()) {
        ViewChangeMessage viewChangeMessage;
        viewChangeMessage.set_sessnum(this->sessnum);
        viewChangeMessage.set_view(this->view);
        viewChangeMessage.set_lastnormalsessnum(this->lastNormalSessnum);
        viewChangeMessage.set_lastnormalview(this->lastNormalView);
        viewChangeMessage.set_lastop(this->lastOp);
        viewChangeMessage.set_replicaidx(this->replicaIdx);
        for (opnum_t gap : this->committedGaps) {
            viewChangeMessage.add_committedgaps(gap);
        }

        if (!this->transport->SendMessageToReplica(this,
                                                   this->configuration.GetLeaderIndex(this->view),
                                                   viewChangeMessage)) {
            RWarning("Failed to send ViewChangeMessage to leader");
        }
    }
    this->viewChangeTimeout->Reset();
}

void
NOPaxosReplica::SendStateTransferRequest()
{
    StateTransferRequestMessage request;
    request.set_sessnum(this->sessnum);
    request.set_view(this->view);
    request.set_begin(this->stateTransferOpBegin);
    request.set_end(this->stateTransferOpEnd);

    RDebug("Leader requesting StateTransfer from %lu to %lu from replica %u",
           this->stateTransferOpBegin,
           this->stateTransferOpEnd,
           this->stateTransferReplicaIdx);
    if (this->stateTransferOpEnd - this->stateTransferOpBegin > MAX_SYNC_TRANSFER_OPS) {
        RWarning("Sending large state transfer request %lu ops",
                 this->stateTransferOpEnd - this->stateTransferOpBegin);
    }
    if (!(this->transport->SendMessageToReplica(this,
                                                this->stateTransferReplicaIdx,
                                                request))) {
        RWarning("Failed to send StateTransferRequestMessage during view change");
    }
    this->stateTransferTimeout->Reset();
}

void
NOPaxosReplica::SendStartView()
{
    ASSERT(AmLeader());
    StartViewMessage startViewMessage;
    startViewMessage.set_sessnum(this->sessnum);
    startViewMessage.set_view(this->view);
    startViewMessage.set_lastnormalsessnum(this->startViewLastNormalSessnum);
    startViewMessage.set_lastnormalview(this->startViewLastNormalView);
    startViewMessage.set_lastop(this->startViewLastOp);
    for (opnum_t gap : this->startViewCommittedGaps) {
        startViewMessage.add_committedgaps(gap);
    }

    if (!(this->transport->SendMessageToAll(this, startViewMessage))) {
        RWarning("Failed to send StartViewMessage to all replicas");
    }
    this->startViewTimeout->Reset();
}

void
NOPaxosReplica::SendStartViewReply()
{
    ASSERT(!AmLeader());
    ASSERT(this->status != STATUS_VIEW_CHANGE);

    StartViewReplyMessage startViewReplyMessage;
    startViewReplyMessage.set_sessnum(this->sessnum);
    startViewReplyMessage.set_view(this->view);
    startViewReplyMessage.set_replicaidx(this->replicaIdx);

    if (!this->transport->SendMessageToReplica(this,
                                               this->configuration.GetLeaderIndex(this->view),
                                               startViewReplyMessage)) {
        RWarning("Failed to send StartViewReplyMessage to leader");
    }
}

void
NOPaxosReplica::SendSyncPrepare()
{
    ASSERT(AmLeader());

    this->leaderLastSyncPreparePoint = this->lastOp;
    this->leaderLastSyncCommittedGaps = this->committedGaps;
    RDebug("Send SyncPrepare for syncpoint %lu", this->leaderLastSyncPreparePoint);
    SyncPrepareMessage syncPrepareMessage;
    syncPrepareMessage.set_sessnum(this->sessnum);
    syncPrepareMessage.set_view(this->view);
    syncPrepareMessage.set_lastop(this->lastOp);
    for (opnum_t gap : this->leaderLastSyncCommittedGaps) {
        syncPrepareMessage.add_committedgaps(gap);
    }

    if (!this->transport->SendMessageToAll(this, syncPrepareMessage)) {
        RWarning("Failed to send SyncPrepare");
    }
}

void
NOPaxosReplica::SendSyncCommit()
{
    ASSERT(AmLeader());

    SyncCommitMessage syncCommitMessage;
    syncCommitMessage.set_sessnum(this->sessnum);
    syncCommitMessage.set_view(this->view);
    syncCommitMessage.set_syncpoint(this->lastCommittedOp);

    if (!this->transport->SendMessageToAll(this, syncCommitMessage)) {
        RWarning("Failed to send SyncCommitMessage");
    }
}

inline bool
NOPaxosReplica::AmLeader() const
{
    return (this->configuration.GetLeaderIndex(this->view) == this->replicaIdx);
}

inline void
NOPaxosReplica::AddPendingRequest(const RequestMessage &msg)
{
    if (msg.sessnum() >= this->sessnum &&
        msg.msgnum() >= this->nextMsgnum) {
        this->pendingRequests.push_back(msg);
        this->pendingRequestsSorted = false;
    }
}

inline bool
NOPaxosReplica::CheckViewNumAndStatus(sessnum_t sessnum, view_t view)
{
    if (sessnum < this->sessnum || view < this->view) {
        return false;
    }

    if (sessnum > this->sessnum || view > this->view) {
        StartViewChange(sessnum, view);
        return false;
    }

    if (this->status == STATUS_VIEW_CHANGE) {
        return false;
    }

    return true;
}

inline void
NOPaxosReplica::RewindLogToOpnum(opnum_t opnum)
{
    ASSERT(opnum >= this->lastCommittedOp);
    if (opnum < this->lastExecutedOp) {
        RPanic("Trying to rewind operation that has already executed. However rollback is \
               not implemented yet");
    }
    this->lastOp = opnum;
    this->log.RemoveAfter(opnum);
    this->nextMsgnum = this->log.LastViewstamp().msgnum + 1;
}

} // namespace specpaxos::nopaxos
} // namespace specpaxos
