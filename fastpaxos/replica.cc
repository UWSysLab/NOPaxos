// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fastpaxos/replica.cc:
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

#include "common/replica.h"
#include "fastpaxos/replica.h"
#include "fastpaxos/fastpaxos-proto.pb.h"

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"

#include <algorithm>

#define RDebug(fmt, ...) Debug("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)

namespace specpaxos {
namespace fastpaxos {

using namespace proto;

FastPaxosReplica::FastPaxosReplica(Configuration config, int myIdx,
                                   bool initialize,
                                   Transport *transport,
                                   AppReplica *app)
    : Replica(config, 0, myIdx, initialize, transport, app),
      log(false),
      slowPrepareOKQuorum(config.QuorumSize()-1),
      fastPrepareOKQuorum(config.FastQuorumSize()-1)
{
    if (!initialize) {
        RPanic("Recovery not implemented");
    }

    this->status = STATUS_NORMAL;
    this->view = 0;
    this->lastFastPath = 0;
    this->lastSlowPath = 0;
    this->lastCommitted = 0;
    this->lastRequestStateTransferView = 0;
    this->lastRequestStateTransferOpnum = 0;


    this->stateTransferTimeout = new Timeout(transport, 1000, [this]() {
            this->lastRequestStateTransferView = 0;
            this->lastRequestStateTransferOpnum = 0;
        });
    this->stateTransferTimeout->Start();
    this->resendPrepareTimeout = new Timeout(transport, 500, [this]() {
            ResendPrepare();
        });

}

FastPaxosReplica::~FastPaxosReplica()
{
    delete stateTransferTimeout;
    delete resendPrepareTimeout;

    for (auto &kv : pendingPrepares) {
        delete kv.first;
    }
}

bool
FastPaxosReplica::AmLeader() const
{
    return (configuration.GetLeaderIndex(view) == this->replicaIdx);
}

void
FastPaxosReplica::CommitUpTo(opnum_t upto)
{
    while (lastCommitted < upto) {
        lastCommitted++;

        /* Find operation in log */
        const LogEntry *entry = log.Find(lastCommitted);
        if (!entry) {
            RPanic("Did not find operation " FMT_OPNUM " in log", lastCommitted);
        }

        /* Execute it */
        RDebug("Executing request " FMT_OPNUM, lastCommitted);
        ReplyMessage reply;
        Execute(lastCommitted, entry->request, reply);

        reply.set_view(entry->viewstamp.view);
        reply.set_opnum(entry->viewstamp.opnum);
        reply.set_clientreqid(entry->request.clientreqid());

        /* Mark it as committed */
        log.SetStatus(lastCommitted, LOG_STATE_COMMITTED);

        // Store reply in the client table
        ClientTableEntry &cte =
            clientTable[entry->request.clientid()];
        if (cte.lastReqId <= entry->request.clientreqid()) {
            cte.lastReqId = entry->request.clientreqid();
            cte.replied = true;
            cte.reply = reply;
        } else {
            // We've subsequently prepared another operation from the
            // same client. So this request must have been completed
            // at the client, and there's no need to record the
            // result.
        }

        /* Send reply */
        auto iter = clientAddresses.find(entry->request.clientid());
        if (iter != clientAddresses.end()) {
            transport->SendMessage(this, *iter->second, reply);
        }
    }

    if (lastSlowPath < lastCommitted) {
        lastSlowPath = lastCommitted;
    }
    ASSERT(lastFastPath >= lastCommitted);
}

void
FastPaxosReplica::SendPrepareOKs(opnum_t oldLastOp)
{
    /* Send PREPAREOKs for new uncommitted operations */
    for (opnum_t i = oldLastOp; i <= lastSlowPath; i++) {
        /* It has to be new *and* uncommitted */
        if (i <= lastCommitted) {
            continue;
        }

        const LogEntry *entry = log.Find(i);
        if (!entry) {
            RPanic("Did not find operation " FMT_OPNUM " in log", i);
        }
        ASSERT(entry->state == LOG_STATE_PREPARED);
        UpdateClientTable(entry->request);

        PrepareOKMessage reply;
        reply.set_view(view);
        reply.set_opnum(i);
        reply.set_replicaidx(this->replicaIdx);
        reply.set_slowpath(1);
        *(reply.mutable_req()) = entry->request;

        RDebug("Sending PREPAREOK " FMT_VIEWSTAMP " for new uncommitted operation",
               reply.view(), reply.opnum());

        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(view),
                                              reply))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
    }
}

void
FastPaxosReplica::RequestStateTransfer()
{
    RequestStateTransferMessage m;
    m.set_view(view);
    m.set_opnum(lastCommitted);

    if ((lastRequestStateTransferOpnum != 0) &&
        (lastRequestStateTransferView == view) &&
        (lastRequestStateTransferOpnum == lastCommitted)) {
        RDebug("Skipping state transfer request " FMT_VIEWSTAMP
               " because we already requested it", view, lastCommitted);
        return;
    }

    RNotice("Requesting state transfer: " FMT_VIEWSTAMP, view, lastCommitted);

    this->lastRequestStateTransferView = view;
    this->lastRequestStateTransferOpnum = lastCommitted;

    if (!transport->SendMessageToAll(this, m)) {
        RWarning("Failed to send RequestStateTransfer message to all replicas");
    }
}

void
FastPaxosReplica::EnterView(view_t newview)
{
    RNotice("Entering new view " FMT_VIEW, newview);

    view = newview;
    status = STATUS_NORMAL;

    fastPrepareOKQuorum.Clear();
    slowPrepareOKQuorum.Clear();
}

void
FastPaxosReplica::UpdateClientTable(const Request &req)
{
    ClientTableEntry &entry = clientTable[req.clientid()];

    if (entry.lastReqId >= req.clientreqid()) {
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.replied = false;
    entry.reply.Clear();
}

void
FastPaxosReplica::ResendPrepare()
{
    ASSERT(AmLeader());
    if (lastSlowPath == lastCommitted) {
        return;
    }
    RNotice("Resending prepare");
    resendPrepareTimeout->Reset();
    if (!(transport->SendMessageToAll(this, lastPrepare))) {
        RWarning("Failed to ressend prepare message to all replicas");
    }
}

void
FastPaxosReplica::ReceiveMessage(const TransportAddress &remote,
                                 const string &type, const string &data,
                                 void *meta_data)
{
    static RequestMessage request;
    static UnloggedRequestMessage unloggedRequest;
    static PrepareMessage prepare;
    static PrepareOKMessage prepareOK;
    static CommitMessage commit;
    static RequestStateTransferMessage requestStateTransfer;
    static StateTransferMessage stateTransfer;

    if (type == request.GetTypeName()) {
        request.ParseFromString(data);
        HandleRequest(remote, request);
    } else if (type == unloggedRequest.GetTypeName()) {
        unloggedRequest.ParseFromString(data);
        HandleUnloggedRequest(remote, unloggedRequest);
    } else if (type == prepare.GetTypeName()) {
        prepare.ParseFromString(data);
        HandlePrepare(remote, prepare);
    } else if (type == prepareOK.GetTypeName()) {
        prepareOK.ParseFromString(data);
        HandlePrepareOK(remote, prepareOK);
    } else if (type == commit.GetTypeName()) {
        commit.ParseFromString(data);
        HandleCommit(remote, commit);
    } else if (type == requestStateTransfer.GetTypeName()) {
        requestStateTransfer.ParseFromString(data);
        HandleRequestStateTransfer(remote, requestStateTransfer);
    } else if (type == stateTransfer.GetTypeName()) {
        stateTransfer.ParseFromString(data);
        HandleStateTransfer(remote, stateTransfer);
    } else {
        RPanic("Received unexpected message type in FastPaxos proto: %s",
               type.c_str());
    }
}

void
FastPaxosReplica::HandleRequest(const TransportAddress &remote,
                                const RequestMessage &msg)
{
    viewstamp_t v;

    if (status != STATUS_NORMAL) {
        RNotice("Ignoring request due to abnormal status");
        return;
    }

    // Save the client's address
    clientAddresses.erase(msg.req().clientid());
    clientAddresses.insert(
        std::pair<uint64_t, std::unique_ptr<TransportAddress> >(
            msg.req().clientid(),
            std::unique_ptr<TransportAddress>(remote.clone())));

    // Check the client table to see if this is a duplicate request
    auto kv = clientTable.find(msg.req().clientid());
    if (kv != clientTable.end()) {
        const ClientTableEntry &entry = kv->second;
        if (msg.req().clientreqid() < entry.lastReqId) {
            RNotice("Ignoring stale request");
            return;
        }
        if (msg.req().clientreqid() == entry.lastReqId) {
            // This is a duplicate request. Resend the reply if we
            // have one. We might not have a reply to resend if we're
            // waiting for the other replicas; in that case, just
            // discard the request.
            if (entry.replied) {
                RNotice("Received duplicate request; resending reply");
                if (!(transport->SendMessage(this, remote,
                                             entry.reply))) {
                    RWarning("Failed to resend reply to client");
                }
                return;
            } else {
                RNotice("Received duplicate request but no reply available; ignoring");
                return;
            }
        }
    }

    // Update the client table
    UpdateClientTable(msg.req());

    /* Assign it an opnum */
    if (AmLeader()) {
        ASSERT(lastFastPath == lastSlowPath);
        ++lastSlowPath;
        v.view = view;
        v.opnum = lastSlowPath;
        lastFastPath = lastSlowPath;
    } else {
        ++lastFastPath;
        v.view = view;
        v.opnum = lastFastPath;
    }

    RDebug("Received REQUEST, assigning " FMT_VIEWSTAMP, VA_VIEWSTAMP(v));

    /* Add the request to my log */
    log.Append(v, msg.req(), LOG_STATE_PREPARED);

    if (AmLeader()) {
        /* Prepare a prepare message */
        PrepareMessage p;
        p.set_view(v.view);
        p.set_opnum(v.opnum);
        *(p.mutable_req()) = msg.req();
        lastPrepare = p;

        // ...but don't actually send it, as an optimization. We'll
        // only send it if the timeout fires or we get a conflicting
        // reply.

        resendPrepareTimeout->Reset();

        // Process pending prepare oks
        std::list<std::pair<TransportAddress *, PrepareOKMessage> >pending
            = pendingPrepareOKs;
        pendingPrepareOKs.clear();
        for (auto & msgpair : pending) {
            RDebug("Processing pending prepareOK message");
            HandlePrepareOK(*msgpair.first, msgpair.second);
            delete msgpair.first;
        }

    } else {
        // Send fast-path prepareOK message to leader
        PrepareOKMessage pok;
        pok.set_view(v.view);
        pok.set_opnum(v.opnum);
        *(pok.mutable_req()) = msg.req();
        pok.set_replicaidx(this->replicaIdx);
        pok.set_slowpath(0);

        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(view),
                                              pok))) {
            RWarning("Failed to send fast-path PrepareOK message to leader");
        }
    }
}

void
FastPaxosReplica::HandleUnloggedRequest(const TransportAddress &remote,
                                        const UnloggedRequestMessage &msg)
{
    if (status != STATUS_NORMAL) {
        // Not clear if we should ignore this or just let the request
        // go ahead, but this seems reasonable.
        RNotice("Ignoring unlogged request due to abnormal status");
        return;
    }

    UnloggedReplyMessage reply;

    Debug("Received unlogged request %s", (char *)msg.req().op().c_str());

    ExecuteUnlogged(msg.req(), reply);

    if (!(transport->SendMessage(this, remote, reply)))
        Warning("Failed to send reply message");
}

void
FastPaxosReplica::HandlePrepare(const TransportAddress &remote,
                                const PrepareMessage &msg)
{
    RDebug("Received PREPARE <" FMT_VIEW "," FMT_OPNUM ">", msg.view(), msg.opnum());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPARE due to abnormal status");
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring PREPARE due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
        return;
    }

    if (AmLeader()) {
        RPanic("Unexpected PREPARE: I'm the leader of this view");
    }

    if (msg.opnum() <= this->lastSlowPath) {
        RDebug("Ignoring PREPARE; already prepared that operation");
        // Resend the prepareOK message
        PrepareOKMessage reply;
        reply.set_view(msg.view());
        reply.set_opnum(msg.opnum());
        reply.set_replicaidx(this->replicaIdx);
        reply.set_slowpath(1);
        *(reply.mutable_req()) = msg.req();
        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(view),
                                              reply))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
        return;
    }

    if (msg.opnum() > this->lastSlowPath+1) {
        RequestStateTransfer();
        pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
        return;
    }

    /* Add operation to the log, replacing existing entry as needed */
    if (msg.opnum() > lastFastPath) {
        ASSERT(msg.opnum() == lastFastPath+1);
        ++lastFastPath;
        ++lastSlowPath;
        log.Append(viewstamp_t(msg.view(), msg.opnum()),
                   msg.req(), LOG_STATE_PREPARED);
    } else {
        ASSERT(msg.opnum() == lastSlowPath+1);
        const LogEntry *entry = log.Find(msg.opnum());
        ASSERT(entry != NULL);
        ASSERT(entry->state == LOG_STATE_PREPARED);
        log.SetRequest(msg.opnum(), msg.req());
        ++lastSlowPath;
    }

    UpdateClientTable(msg.req());

    /* Build reply and send it to the leader */
    PrepareOKMessage reply;
    reply.set_view(msg.view());
    reply.set_opnum(msg.opnum());
    reply.set_replicaidx(this->replicaIdx);
    reply.set_slowpath(1);
    *(reply.mutable_req()) = msg.req();

    if (!(transport->SendMessageToReplica(this,
                                          configuration.GetLeaderIndex(view),
                                          reply))) {
        RWarning("Failed to send PrepareOK message to leader");
    }
}

void
FastPaxosReplica::HandlePrepareOK(const TransportAddress &remote,
                                  const PrepareOKMessage &msg)
{    RDebug("Received %s PREPAREOK " FMT_VIEWSTAMP " from replica %d",
           msg.slowpath() ? "slow-path" : "fast-path",
           msg.view(), msg.opnum(), msg.replicaidx());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPAREOK due to abnormal status");
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring PREPAREOK due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        return;
    }

    if (!AmLeader()) {
        RWarning("Ignoring PREPAREOK because I'm not the leader");
        return;
    }

    if (msg.opnum() <= lastCommitted) {
        // Already committed this operation
        RDebug("Ignoring PREPAREOK; already committed");
        return;
    }

    if (msg.opnum() > lastSlowPath) {
        pendingPrepareOKs.push_back(std::make_pair(remote.clone(), msg));
        Debug("Deferring PREPAREOK because we haven't received the request yet");
        return;
    }

    const LogEntry *entry = log.Find(msg.opnum());
    ASSERT(entry != NULL);
    ASSERT(entry->state == LOG_STATE_PREPARED);

    if (!msg.slowpath()) {
        // Fast-path response. Make sure the request matches what we
        // have.
        if ((entry->request.clientid() != msg.req().clientid()) ||
            (entry->request.clientreqid() != msg.req().clientreqid())) {
            RNotice("Discarding conflicting fast-path response");

            // Send the PREPARE message in case we haven't already.
            // XXX We might not want to do this for *each* conflicting
            // fast-path response, but it should be OK.
            ResendPrepare();
            return;
        }
#if PARANOID
        ASSERT(entry->request.op() == msg.req().op());
#endif
    } else {
        // Slow-path response is guaranteed to match ours.
#if PARANOID
        ASSERT(entry->request.clientid() == msg.req().clientid());
        ASSERT(entry->request.clientreqid() == msg.req().clientreqid());
        ASSERT(entry->request.op() == msg.req().op());
#endif
    }

    auto &quorum = msg.slowpath() ? slowPrepareOKQuorum : fastPrepareOKQuorum;

    viewstamp_t vs = { msg.view(), msg.opnum() };
    if (auto msgs =
        (quorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg))) {
        RDebug("Received quorum of PREPAREOK messages");
        /*
         * We have a quorum of PrepareOK messages for this
         * opnumber. Execute it and all previous operations.
         *
         * (Note that we might have already executed it. That's fine,
         * we just won't do anything.)
         *
         * This also notifies the client of the result.
         */
        CommitUpTo(msg.opnum());

        if (msgs->size() > (unsigned int)quorum.NumRequired()) {
            return;
        }

        /*
         * Send COMMIT message to the other replicas.
         *
         * This can be done asynchronously, so it really ought to be
         * piggybacked on the next PREPARE or something.
         */
        CommitMessage cm;
        cm.set_view(view);
        cm.set_opnum(lastCommitted);
        *(cm.mutable_req()) = entry->request;

        if (!(transport->SendMessageToAll(this, cm))) {
            RWarning("Failed to send COMMIT message to all replicas");
        }
    }
}

void
FastPaxosReplica::HandleCommit(const TransportAddress &remote,
                               const CommitMessage &msg)
{
    RDebug("Received COMMIT " FMT_VIEWSTAMP, msg.view(), msg.opnum());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring COMMIT due to abnormal status");
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring COMMIT due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        return;
    }

    if (AmLeader()) {
        RPanic("Unexpected COMMIT: I'm the leader of this view");
    }

    if (msg.opnum() <= this->lastCommitted) {
        RDebug("Ignoring COMMIT; already committed that operation");
        return;
    }

    if (msg.opnum() > this->lastFastPath) {
        RequestStateTransfer();
        return;
    }

    if (msg.opnum() > this->lastSlowPath) {
        // We can only commit a fast-path operation if it's the most
        // recent one, since we need to make sure we have the right
        // value.
        if (msg.opnum() > this->lastSlowPath+1) {
            RNotice("Ignoring out-of-order fast-path commit");
            return;
        }
        const LogEntry *entry = log.Find(msg.opnum());
        ASSERT(entry != NULL);
        ASSERT(entry->state == LOG_STATE_PREPARED);
        log.SetRequest(msg.opnum(), msg.req());
    }

    CommitUpTo(msg.opnum());
}


void
FastPaxosReplica::HandleRequestStateTransfer(const TransportAddress &remote,
                                             const RequestStateTransferMessage &msg)
{
    RDebug("Received REQUESTSTATETRANSFER " FMT_VIEWSTAMP,
           msg.view(), msg.opnum());

    if (status != STATUS_NORMAL) {
        RDebug("Ignoring REQUESTSTATETRANSFER due to abnormal status");
        return;
    }

    if (msg.view() > view) {
        RequestStateTransfer();
        return;
    }

    if (!AmLeader()) {
        return;
    };

    RNotice("Sending state transfer from " FMT_VIEWSTAMP " to "
            FMT_VIEWSTAMP,
            msg.view(), msg.opnum(), view, lastCommitted);

    StateTransferMessage reply;
    reply.set_view(view);
    reply.set_opnum(lastCommitted);
    reply.set_lastop(lastSlowPath);
    ASSERT(lastSlowPath == lastFastPath);

    log.Dump(msg.opnum()+1, reply.mutable_entries());

    transport->SendMessage(this, remote, reply);
}

void
FastPaxosReplica::HandleStateTransfer(const TransportAddress &remote,
                                      const StateTransferMessage &msg)
{
    RDebug("Received STATETRANSFER " FMT_VIEWSTAMP " to " FMT_OPNUM,
           msg.view(), msg.opnum(), msg.lastop());

    if (msg.view() < view) {
        RWarning("Ignoring state transfer for older view");
        return;
    }

    opnum_t oldLastSlowPath = lastSlowPath;

    /* Install the new log entries */
    for (auto newEntry : msg.entries()) {
        if (newEntry.opnum() <= lastCommitted) {
            // Already committed this operation; nothing to be done.
#if PARANOID
            const LogEntry *entry = log.Find(newEntry.opnum());
            ASSERT(entry->viewstamp.opnum == newEntry.opnum());
            ASSERT(entry->viewstamp.view == newEntry.view());
//          ASSERT(entry->request == newEntry.request());
#endif
        } else if (newEntry.opnum() <= lastSlowPath) {
            // We already have an entry with this opnum, but maybe
            // it's from an older view?
            const LogEntry *entry = log.Find(newEntry.opnum());
            ASSERT(entry->viewstamp.opnum == newEntry.opnum());
            ASSERT(entry->viewstamp.view <= newEntry.view());

            if (entry->viewstamp.view == newEntry.view()) {
                // We already have this operation in our log.
                ASSERT(entry->state == LOG_STATE_PREPARED);
#if PARANOID
//              ASSERT(entry->request == newEntry.request());
#endif
            } else {
                // Our operation was from an older view, so obviously
                // it didn't survive a view change. Throw out any
                // later log entries and replace with this one.
                ASSERT(entry->state != LOG_STATE_COMMITTED);
                log.RemoveAfter(newEntry.opnum());
                lastSlowPath = newEntry.opnum();
                oldLastSlowPath = lastSlowPath;

                viewstamp_t vs = { newEntry.view(), newEntry.opnum() };
                log.Append(vs, newEntry.request(), LOG_STATE_PREPARED);
            }
        } else if (newEntry.opnum() <= lastFastPath) {
            // This is a new slow-path operation, but we already have
            // a fast-path operation. Replace the existing log entry.
            const LogEntry *entry = log.Find(newEntry.opnum());
            ASSERT(entry != NULL);
            ASSERT(entry->state == LOG_STATE_PREPARED);
            log.SetRequest(newEntry.opnum(), newEntry.request());
            lastSlowPath++;
        } else {
            // This is a totally new operation to us. Add it to the log.
            ASSERT(newEntry.opnum() == lastSlowPath+1);

            lastSlowPath++;
            lastFastPath++;
            viewstamp_t vs = { newEntry.view(), newEntry.opnum() };
            log.Append(vs, newEntry.request(), LOG_STATE_PREPARED);
        }
    }


    if (msg.view() > view) {
        EnterView(msg.view());
    }

    /* Execute committed operations */
    ASSERT(msg.opnum() <= lastSlowPath);
    ASSERT(lastSlowPath <= lastFastPath);
    CommitUpTo(msg.opnum());
    SendPrepareOKs(oldLastSlowPath);

    // Process pending prepares
    std::list<std::pair<TransportAddress *, PrepareMessage> >pending = pendingPrepares;
    pendingPrepares.clear();
    for (auto & msgpair : pending) {
        RDebug("Processing pending prepare message");
        HandlePrepare(*msgpair.first, msgpair.second);
        delete msgpair.first;
    }
}

} // namespace specpaxos::fastpaxos
} // namespace specpaxos
