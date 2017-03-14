// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * spec/replica.cc:
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

#define SYNC_TIMEOUT_MS            1000
#define SYNC_TIMEOUT_REQUESTS      10000
#define VIEW_CHANGE_TIMEOUT_MS     5000

#include "common/replica.h"
#include "spec/replica.h"
#include "spec/spec-proto.pb.h"

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"

#include <algorithm>
#include <map>
#include <set>
#include <unordered_set>
#include <vector>

#define RDebug(fmt, ...) Debug("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)

namespace std {
template <> struct hash<pair<uint64_t, uint64_t> >
{
    size_t operator()(const pair<uint64_t, uint64_t> & x) const
        {
            return x.first * 37 + x.second;
        }
};

}

namespace specpaxos {
namespace spec {

const opnum_t LOG_FUDGE = 20;
const opnum_t DVC_FUDGE = 25;

using namespace specpaxos::spec::proto;

SpecReplica::SpecReplica(Configuration config, int myIdx,
                         bool initialize,
                         Transport *transport, AppReplica *app)
    : Replica(config, 0, myIdx, initialize, transport, app),
      log(true),
      syncReplyQuorum(config.FastQuorumSize()-1),
      startViewChangeQuorum(config.QuorumSize()-1),
      doViewChangeQuorum(config.QuorumSize()),
      inViewQuorum(config.QuorumSize()-1)
{
    if (!initialize) {
        RPanic("Recovery not implemented");
    }

    this->status = STATUS_NORMAL;
    this->view = 0;
    this->lastSpeculative = 0;
    this->lastCommitted = 0;
    this->lastCommittedSent = 0;
    this->sentDoViewChange = 0;
    this->needFillDVC = 0;
    this->pendingSync = 0;
    this->lastSync = 0;

    this->syncTimeout = new Timeout(transport,
                                    SYNC_TIMEOUT_MS,
                                    [this]() {
            SendSync();
        });
    this->failedSyncTimeout = new Timeout(transport,
                                          VIEW_CHANGE_TIMEOUT_MS,
                                          [&]() {
            Warning("Failed to get a quorum of SYNCREPLYs");
            StartViewChange(view+1);
        });
    this->viewChangeTimeout = new Timeout(transport,
                                          VIEW_CHANGE_TIMEOUT_MS,
                                          [this]() {
            Notice("Starting view change; haven't received SYNC");
            StartViewChange(view+1);
        });

    _Latency_Init(&reconciliationLatency, "reconciliation");
    _Latency_Init(&mergeLatency, "merge");
    _Latency_Init(&requestLatency, "request");

    EnterView(0);
}

SpecReplica::~SpecReplica()
{
    Latency_Dump(&requestLatency);
    Latency_Dump(&reconciliationLatency);
    Latency_Dump(&mergeLatency);

    delete syncTimeout;
    delete failedSyncTimeout;
    delete viewChangeTimeout;
}

bool
SpecReplica::AmLeader() const
{
    return (configuration.GetLeaderIndex(view) == this->replicaIdx);
}

void
SpecReplica::CommitUpTo(opnum_t upto)
{
    RNotice("Committing up to " FMT_OPNUM, upto);

    while (lastCommitted < upto) {
        lastCommitted++;

        /* Mark it as committed */
        bool success = log.SetStatus(lastCommitted, LOG_STATE_COMMITTED);
        if (!success) {
            RPanic("Entry not found in log");
        }
    }

    Commit(upto);
}

void
SpecReplica::RollbackTo(opnum_t backto)
{
    if (backto >= lastSpeculative) {
        return;
    }

    ASSERT(backto >= lastCommitted);

    RNotice("Rolling back from " FMT_OPNUM " to " FMT_OPNUM,
           lastSpeculative, backto);

    for (opnum_t i = lastSpeculative; i > backto; i--) {
        // Roll back the client table
        const LogEntry *entry = log.Find(i);
        ASSERT(entry != NULL);

        ClientTableEntry &cte = clientTable[entry->request.clientid()];
        ASSERT(cte.lastReqOpnum == entry->viewstamp.opnum);
        cte.lastReqOpnum = entry->prevClientReqOpnum;
        if (cte.lastReqOpnum > 0) {
            const LogEntry *oldEntry = log.Find(cte.lastReqOpnum);
            ASSERT(oldEntry != NULL);
            RDebug("Rolling back client table entry for %lx from %ld to %ld",
                    entry->request.clientid(),
                    cte.lastReqId,
                    oldEntry->request.clientreqid());
            cte.lastReqId = oldEntry->request.clientreqid();
        } else {
            cte.lastReqId = 0;
        }
    }

    Rollback(lastSpeculative, backto, log);


    log.RemoveAfter(backto+1);
    lastSpeculative = backto;
}

void
SpecReplica::UpdateClientTable(const Request &req,
                               LogEntry &logEntry,
                               const SpeculativeReplyMessage &reply)
{
    ClientTableEntry &entry = clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    logEntry.prevClientReqOpnum = entry.lastReqOpnum;
    logEntry.replyMessage = new SpeculativeReplyMessage(reply);

    if (entry.lastReqId == req.clientreqid()) {
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.lastReqOpnum = logEntry.viewstamp.opnum;
}

void
SpecReplica::ReceiveMessage(const TransportAddress &remote,
                            const string &type, const string &data,
                            void *meta_data)
{
    static RequestMessage request;
    static UnloggedRequestMessage unloggedRequest;
    static SyncMessage sync;
    static SyncReplyMessage syncReply;
    static StartViewChangeMessage startViewChange;
    static DoViewChangeMessage doViewChange;
    static StartViewMessage startView;
    static InViewMessage inView;
    static FillLogGapMessage fillLogGap;
    static FillDVCGapMessage fillDVCGap;
    static RequestViewChangeMessage requestViewChange;

    if (type == request.GetTypeName()) {
        request.ParseFromString(data);
        HandleRequest(remote, request);
    } else if (type == unloggedRequest.GetTypeName()) {
        unloggedRequest.ParseFromString(data);
        HandleUnloggedRequest(remote, unloggedRequest);
    } else if (type == sync.GetTypeName()) {
        sync.ParseFromString(data);
        HandleSync(remote, sync);
    } else if (type == syncReply.GetTypeName()) {
        syncReply.ParseFromString(data);
        HandleSyncReply(remote, syncReply);
    } else if (type == startViewChange.GetTypeName()) {
        startViewChange.ParseFromString(data);
        HandleStartViewChange(remote, startViewChange);
    } else if (type == doViewChange.GetTypeName()) {
        doViewChange.ParseFromString(data);
        HandleDoViewChange(remote, doViewChange);
    } else if (type == startView.GetTypeName()) {
        startView.ParseFromString(data);
        HandleStartView(remote, startView);
    } else if (type == inView.GetTypeName()) {
        inView.ParseFromString(data);
        HandleInView(remote, inView);
    } else if (type == fillLogGap.GetTypeName()) {
        fillLogGap.ParseFromString(data);
        HandleFillLogGap(remote, fillLogGap);
    } else if (type == fillDVCGap.GetTypeName()) {
        fillDVCGap.ParseFromString(data);
        HandleFillDVCGap(remote, fillDVCGap);
    } else if (type == requestViewChange.GetTypeName()) {
        requestViewChange.ParseFromString(data);
        HandleRequestViewChange(remote, requestViewChange);
    } else {
        RPanic("Received unexpected message type in SPEC proto: %s",
              type.c_str());
    }
}


/*
 * Speculative processing
 */

void
SpecReplica::HandleRequest(const TransportAddress &remote,
                           const RequestMessage &msg)
{
    viewstamp_t v;

    Latency_Start(&requestLatency);

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
            Latency_EndType(&requestLatency, 's');
            return;
        }
        if (msg.req().clientreqid() == entry.lastReqId) {
            // This is a duplicate request. Resend the reply.
            RNotice("Received duplicate request from client %lx; resending reply",
                    msg.req().clientid());
            const LogEntry *le = log.Find(entry.lastReqOpnum);
            ASSERT(le != NULL);
            SpeculativeReplyMessage *reply =
                (SpeculativeReplyMessage *) le->replyMessage;
            ASSERT(reply != NULL);
            if (le->state == LOG_STATE_COMMITTED) {
                reply->set_committed(true);
            }
            if (!(transport->SendMessage(this, remote,
                                         *reply))) {
                RWarning("Failed to resend reply to client");
            }
            Latency_EndType(&requestLatency, 'r');
            return;
        }
    }

    // Make sure we're not doing a view change
    if (status != STATUS_NORMAL) {
        pendingRequests.push_back(
            std::pair<TransportAddress*, RequestMessage>(remote.clone(),
                                                         msg));
        RDebug("Deferring request due to abnormal status");
        Latency_EndType(&requestLatency, 'd');
        return;
    }

    /* Assign it an opnum */
    ++this->lastSpeculative;
    v.view = this->view;
    v.opnum = this->lastSpeculative;

    RDebug("Received REQUEST (%lx, %lu), speculatively executing as "
          FMT_VIEWSTAMP,
          msg.req().clientid(), msg.req().clientreqid(), VA_VIEWSTAMP(v));

    SpeculativeReplyMessage reply;
    reply.set_clientreqid(msg.req().clientreqid());
    reply.set_view(v.view);
    reply.set_opnum(v.opnum);
    reply.set_replicaidx(this->replicaIdx);
    reply.set_committed(false);

    /* Add the request to my log and speculatively execute it */
    LogEntry &newEntry =
        log.Append(v, msg.req(), LOG_STATE_SPECULATIVE);
    Execute(v.opnum, msg.req(), reply);

    reply.set_loghash(log.LastHash());

    if (!(transport->SendMessage(this, remote, reply))) {
        RWarning("Failed to send speculative reply");
    }

    // Update the client table
    UpdateClientTable(msg.req(), newEntry, reply);

    Latency_End(&requestLatency);

    // Send a sync reply if we have already received a sync request
    // for this opnum
    if (v.opnum == pendingSync) {
        SendSyncReply(pendingSync);
    }

    // Check whether we need to send a sync based on number of
    // requests, if we are the leader
    if (AmLeader() &&
        (v.opnum >= (lastSync + SYNC_TIMEOUT_REQUESTS))) {
        SendSync();
    }
}

void
SpecReplica::HandleUnloggedRequest(const TransportAddress &remote,
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


/*
 * Synchronization
 */

void
SpecReplica::SendSync()
{
    ASSERT(AmLeader());
    ASSERT(status == STATUS_NORMAL);

    lastSync = lastSpeculative;

    if (lastSpeculative == lastCommittedSent) {
        // Nothing to do.
        // But send the sync message anyway so the other replicas know
        // we're still alive.
    }

    RNotice("Starting synchronization for " FMT_OPNUM, lastSpeculative);

    SyncMessage msg;
    msg.set_view(view);
    msg.set_lastcommitted(lastCommitted);
    if (lastCommitted != 0) {
        msg.set_lastcommittedhash(log.Find(lastCommitted)->hash);
    }
    msg.set_lastspeculative(lastSpeculative);

    if (!(transport->SendMessageToAll(this, msg))) {
        RWarning("Failed to send sync message");
    }
}

void
SpecReplica::HandleSync(const TransportAddress &remote,
                        const SyncMessage &msg)
{
    RDebug("Received SYNC <" FMT_VIEW "," FMT_OPNUM ">", msg.view(),
          msg.lastspeculative());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring SYNC due to abnormal status");
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring SYNC due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        // XXX State transfer?
        RNotice("Received sync for future view " FMT_VIEW, msg.view());
        StartViewChange(msg.view()+1);
        return;
    }

    if (AmLeader()) {
        RPanic("Unexpected SYNC: I'm the leader of this view");
    }

    viewChangeTimeout->Reset();

    // Commit operations we now know to be committed... if we're
    // consistent
    if (msg.lastcommitted() > lastSpeculative) {
        RWarning("Sync for committed opnum that hasn't been received yet");
        // XXX State transfer?
        StartViewChange(view+1);
        return;
    }

    if (msg.lastcommitted() > lastCommitted) {
        const LogEntry *entry = log.Find(msg.lastcommitted());
        ASSERT(entry != NULL);

        if (entry->hash == msg.lastcommittedhash()) {
            CommitUpTo(msg.lastcommitted());
        } else {
            // XXX State transfer instead?
            RNotice("Sync for different hash");
            StartViewChange(view+1);
            return;
        }
    }

    if (msg.lastspeculative() > lastSpeculative) {
        // Record the opnum; we'll send a sync once we receive the
        // corresponding request
        RWarning("Sync for opnum that hasn't been received yet");
        pendingSync = msg.lastspeculative();
        return;
    }

    SendSyncReply(msg.lastspeculative());
}

void
SpecReplica::SendSyncReply(opnum_t opnum)
{
    SyncReplyMessage reply;
    reply.set_replicaidx(this->replicaIdx);
    reply.set_view(this->view);
    reply.set_lastspeculative(opnum);

    if (opnum == 0) {
        reply.set_lastspeculativehash("");
    } else {
        const LogEntry *entry = log.Find(opnum);
        ASSERT(entry != NULL);

        reply.set_lastspeculativehash(entry->hash);
    }

    if (!(transport->SendMessageToReplica(this,
                                          configuration.GetLeaderIndex(view),
                                          reply))) {
        RWarning("Failed to send SYNCREPLY");
    }
}

void
SpecReplica::HandleSyncReply(const TransportAddress &remote,
                             const SyncReplyMessage &msg)
{
    RDebug("Received SYNCREPLY <" FMT_VIEW "," FMT_OPNUM ">", msg.view(),
          msg.lastspeculative());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring SYNCREPLY due to abnormal status");
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring SYNCREPLY due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RPanic("Received SYNCREPLY with newer view; should never happen");
        return;
    }

    if (!AmLeader()) {
        RPanic("Unexpected SYNCREPLY: I'm not the leader of this view");
    }

    ASSERT(msg.lastspeculative() <= lastSpeculative);

    // Check if we have a quorum
    if (auto msgs =
        syncReplyQuorum.AddAndCheckForQuorum(msg.lastspeculative(),
                                             msg.replicaidx(),
                                             msg)) {
        failedSyncTimeout->Reset();

        // If we've already committed everything the other replicas
        // had as speculative, then no need to compare the hashes. The
        // only reason we checked for a quorum was so that we could
        // maintain the failed sync timeout.
        if (msg.lastspeculative() <= lastCommitted) {
            return;
        }

        // We have a quorum of n-e responses. Now to find out if
        // there are n-e *matching* responses...
        std::multimap<string, int> hashes;
        for (auto kv : *msgs) {
            hashes.insert(std::pair<string,int>(
                              kv.second.lastspeculativehash(),
                              kv.first));
        }
        // We need to include our hash too, it's not part of the
        // quorumset
        const LogEntry *entry = log.Find(msg.lastspeculative());
        ASSERT(entry != NULL);
        hashes.insert(std::pair<string,int>(entry->hash, this->replicaIdx));

        const string *matchingHash = NULL;
        // Iterate over unique values
        for (auto it = hashes.begin(); it != hashes.end();
             it = hashes.upper_bound(it->first)) {
            int count = hashes.count(it->first);

            if (count >= configuration.FastQuorumSize()) {
                ASSERT(matchingHash == NULL);
                matchingHash = &it->first;
            }
        }

        if (matchingHash) {
            if (entry->hash != *matchingHash) {
                RWarning("SYNC quorum didn't match our state");
                // XXX State transfer?
                StartViewChange(view+1);
            }

            CommitUpTo(msg.lastspeculative());
        } else {
            // XXX Should we wait to see if another matching request
            // could make this a matching quorum?
            RNotice("SYNC found quorum of non-matching responses");
            StartViewChange(view+1);
        }
    }
}

/*
 * Reconciliation / view changes
 */
void
SpecReplica::EnterView(view_t newview)
{
    bool wasNormal = (status == STATUS_NORMAL);

    RNotice("Entering new view " FMT_VIEW, newview);
    view = newview;
    status = STATUS_NORMAL;

    if (AmLeader()) {
        viewChangeTimeout->Stop();
        syncTimeout->Start();
        failedSyncTimeout->Start();
        lastSync = lastCommitted;
    } else {
        viewChangeTimeout->Start();
        syncTimeout->Stop();
        failedSyncTimeout->Stop();
    }

    syncReplyQuorum.Clear();
    startViewChangeQuorum.Clear();
    doViewChangeQuorum.Clear();
    inViewQuorum.Clear();

    pendingSync = 0;

    std::list<std::pair<TransportAddress *, RequestMessage> > msgs =
        pendingRequests;
    pendingRequests.clear();
    if (msgs.size() > 0) {
        RNotice("Processing %zd deferred requests", msgs.size());
        for (auto msgpair : msgs) {
            HandleRequest(*msgpair.first, msgpair.second);
            delete msgpair.first;
        }
    }
    RNotice("Entered view " FMT_VIEW, newview);

    if (newview != 0 && !wasNormal) {
        Latency_End(&reconciliationLatency);
    }
}

void
SpecReplica::StartViewChange(view_t newview)
{
    RNotice("Starting view change for view " FMT_VIEW, newview);

    if (status == STATUS_VIEW_CHANGE) {
        Latency_EndType(&reconciliationLatency, 'f');
    }
    Latency_Start(&reconciliationLatency);

    view = newview;
    status = STATUS_VIEW_CHANGE;

    viewChangeTimeout->Reset();
    syncTimeout->Stop();
    failedSyncTimeout->Stop();


    StartViewChangeMessage m;
    m.set_view(newview);
    m.set_replicaidx(this->replicaIdx);
    m.set_lastcommitted(lastCommitted);

    if (!transport->SendMessageToAll(this, m)) {
        RWarning("Failed to send StartViewChange message to all replicas");
    }
}

void
SpecReplica::HandleRequestViewChange(const TransportAddress &remote,
                                     const RequestViewChangeMessage &msg)
{
    RDebug("Received REQUESTVIEWCHANGE " FMT_VIEW " from client",
           msg.view());

    if (msg.view() != view) {
        RDebug("Ignoring REQUESTVIEWCHANGE for wrong view");
        return;
    }

    RWarning("Initiating view change for view %ld at client's instigation",
            view+1);

    StartViewChange(view+1);
}


void
SpecReplica::HandleStartViewChange(const TransportAddress &remote,
                                   const StartViewChangeMessage &msg)
{
    RNotice("Received STARTVIEWCHANGE " FMT_VIEW " from replica %d",
           msg.view(), msg.replicaidx());

    if (msg.view() < view) {
        RDebug("Ignoring STARTVIEWCHANGE for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RDebug("Ignoring STARTVIEWCHANGE for current view");
        return;
    }

    if ((status != STATUS_VIEW_CHANGE) || (msg.view() > view)) {
        StartViewChange(msg.view());
    }

    ASSERT(msg.view() == view);

    if (auto msgs =
        startViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                   msg.replicaidx(),
                                                   msg)) {
        if (sentDoViewChange == msg.view()) {
            return;
        }

        int leader = configuration.GetLeaderIndex(view);
        if ((this->replicaIdx != leader) && (msgs->find(leader) == msgs->end())) {
            return;
        }

        sentDoViewChange = msg.view();


        RNotice("Have quorum of StartViewChange messages; "
                "sending DoViewChange to replica %d", leader);

        DoViewChangeMessage dvc;
        dvc.set_view(view);
        dvc.set_lastnormalview(log.LastViewstamp().view);
        dvc.set_lastspeculative(lastSpeculative);
        dvc.set_lastcommitted(lastCommitted);
        dvc.set_replicaidx(this->replicaIdx);

        // Figure out how much of the log to include
        opnum_t minCommitted = std::min_element(
            msgs->begin(), msgs->end(),
            [](decltype(*msgs->begin()) a,
               decltype(*msgs->begin()) b) {
                return a.second.lastcommitted() < b.second.lastcommitted();
            })->second.lastcommitted();
        minCommitted = std::min(minCommitted, lastCommitted);
        if (minCommitted > LOG_FUDGE) {
            minCommitted -= LOG_FUDGE;
        } else {
            minCommitted = 1;
        }

        // XXX And then do something totally different instead...
        // which I sadly do not have time to explain here
        //
        // XXX Is this right if we're sending our own log to
        // ourselves? (You might think we could send the whole thing,
        // but it actually costs a lot to serialize the entire log!)
        if (lastSpeculative > DVC_FUDGE) {
            minCommitted = lastSpeculative - DVC_FUDGE;
        } else {
            minCommitted = 1;
        }

        // Dump log
        log.Dump(minCommitted, dvc.mutable_entries());
        ASSERT(lastSpeculative - minCommitted + 1 == (unsigned long)dvc.entries_size());

        if (leader != this->replicaIdx) {
            if (!(transport->SendMessageToReplica(this, leader, dvc))) {
                RWarning("Failed to send DoViewChange message to leader of new view");
            }
        } else {
            doViewChangeQuorum.AddAndCheckForQuorum(msg.view(), this->replicaIdx,
                                                    dvc);
        }
    }
}

void
SpecReplica::MergeLogs(view_t newView, opnum_t maxStart,
                       const std::map<int, DoViewChangeMessage> &dvcs,
                       std::vector<LogEntry> &out)
{
    using std::pair;
    using std::map;
    using std::set;
    using std::unordered_set;

    out.clear();
    ASSERT(dvcs.size() >= (unsigned int)configuration.QuorumSize());
    ASSERT(maxStart <= lastSpeculative);

    RNotice("Merging %lu logs", dvcs.size());
    for (auto &kv : dvcs) {
        const DoViewChangeMessage &dvc = kv.second;
        RDebug("Log from replica %d:", dvc.replicaidx());
        RDebug("  view %ld", dvc.view());
        RDebug("  lastNormalView %ld", dvc.lastnormalview());
        RDebug("  lastSpeculative %ld", dvc.lastspeculative());
        RDebug("  lastCommitted %ld", dvc.lastcommitted());
        for (auto &entry : dvc.entries()) {
            RDebug("  " FMT_VIEWSTAMP " (%lx, %lu) %s " FMT_BLOB,
                   entry.view(), entry.opnum(),
                   entry.request().clientid(), entry.request().clientreqid(),
                   ((entry.state() == LOG_STATE_COMMITTED) ? "committed" : "speculative"),
                   VA_BLOB_STRING(entry.hash()));
        }
        RDebug(" ");
    }

    // First, we are only interested in logs with the highest
    // normal view. Find these.
    view_t latestView = 0;
    std::vector<const DoViewChangeMessage *> latestViewMessages;
    for (auto &kv : dvcs) {
        if (kv.second.lastnormalview() > latestView) {
            latestView = kv.second.lastnormalview();
            latestViewMessages.clear();
        }
        if (kv.second.lastnormalview() == latestView) {
            latestViewMessages.push_back(&kv.second);
        }
    }

    // We'll keep track of anything we've seen
    unordered_set<pair<uint64_t, uint64_t> > seen;

    // Now start with the one with the most committed
    // entries. We'll take all of those.
    auto mostCommitted =
        *(std::max_element(latestViewMessages.begin(),
                           latestViewMessages.end(),
                           [](const DoViewChangeMessage *a,
                              const DoViewChangeMessage *b) {
                               return a->lastcommitted() < b->lastcommitted();
                           }));

    opnum_t next = 0;
    for (auto &entry : mostCommitted->entries()) {
        if (entry.opnum() < maxStart) {
            continue;
        }
        if (entry.opnum() > mostCommitted->lastcommitted()) {
            break;
        }
        ASSERT((entry.state() == LOG_STATE_COMMITTED));
        out.push_back(LogEntry(viewstamp_t(entry.view(), entry.opnum()),
                               LOG_STATE_COMMITTED, entry.request(),
                               entry.hash()));
        seen.insert(pair<uint64_t, uint64_t>(entry.request().clientid(),
                                             entry.request().clientreqid()));
        next = entry.opnum();
    }
    next++;
    // Go through the list of entries and see if we have any
    // speculative entries that have enough responses that we have to
    // include them
    if (next > 1) {
        ASSERT(out.rbegin()->viewstamp.opnum + 1 == next);
    }
    if (next < maxStart) {
        next = maxStart;
    }
    opnum_t maxSpeculative =
        (*std::max_element(latestViewMessages.begin(),
                         latestViewMessages.end(),
                         [](const DoViewChangeMessage *a,
                            const DoViewChangeMessage *b) {
                             return a->lastspeculative() < b->lastspeculative();
                         }))->lastspeculative();
#if PARANOID
    bool lastFound = true;
#endif
    for (; next <= maxSpeculative; next++) {
        map<string, int> hashCount;
        map<string, Request> reqsByHash;
        map<string, view_t> viewnumByHash;

        for (auto &msgptr : latestViewMessages) {
            auto &msg = *msgptr;
            if (msg.lastspeculative() < next) {
                continue;
            }
            opnum_t firstEntryOpnum = msg.entries(0).opnum();
            auto entry = msg.entries(next - firstEntryOpnum);
            ASSERT(entry.opnum() == next);
            //ASSERT(entry.state() == LOG_STATE_SPECULATIVE);

            if (hashCount.find(entry.hash()) != hashCount.end()) {
                hashCount[entry.hash()] += 1;
#if PARANOID
                ASSERT(entry.request().op() ==
                       reqsByHash[entry.hash()].op());
                ASSERT(entry.view() == viewnumByHash[entry.hash()]);
#endif
            } else {
                hashCount[entry.hash()] = 1;
                reqsByHash[entry.hash()] = entry.request();
                viewnumByHash[entry.hash()] = entry.view();
            }
        }

        int matchingEntriesNeeded =
            std::min((configuration.FastQuorumSize() - configuration.f),
                     (int)latestViewMessages.size());
        bool found = false;
        for (auto &kv : hashCount) {
            if (kv.second >= matchingEntriesNeeded) {
                ASSERT(!found);
                found = true;
                out.push_back(LogEntry(viewstamp_t(viewnumByHash[kv.first],
                                                   next),
                                       LOG_STATE_SPECULATIVE,
                                       reqsByHash[kv.first],
                                       kv.first));
                seen.insert(pair<uint64_t, uint64_t>(reqsByHash[kv.first].clientid(),
                                                     reqsByHash[kv.first].clientreqid()));

#if !PARANOID
                break;
#endif
            }
        }

        // If we didn't find a matching quorum of entries, then unless
        // something is really wrong we won't find a matching quorum
        // of entries for any future opnum. In PARANOID mode, continue
        // searching and make sure we don't find any. Otherwise, just
        // stop now.
#if PARANOID
        if (!lastFound) {
            ASSERT(!found);
        }
        lastFound = found;
#else
        if (!found) {
            break;
        }
#endif
    }

    // Now find any speculative operations from the last view that we
    // have not already committed (in some form or another), and add
    // them to the new log *in the new view and in speculative
    // state*. The order doesn't matter.
    //
    // XXX This is not correct! Some of these entries might be
    // duplicates of operations we've previously executed. Normally,
    // the seen check would catch that, but if the logs are truncated
    // we won't necessarily know. Maybe instead just buffer these to a
    // separate output and then process them through the normal
    // request path before sending the StartView???
    string lastHash = Log::EMPTY_HASH;
    if (out.empty()) {
        next = 1;
    } else {
        next = out.rbegin()->viewstamp.opnum + 1;
        lastHash = out.rbegin()->hash;
    }

    for (auto &msgptr : latestViewMessages) {
        auto &msg = *msgptr;
        for (opnum_t i = std::max(msg.lastcommitted()+1, maxStart);
             i <= msg.lastspeculative(); i++) {
            opnum_t firstEntryOpnum = msg.entries(0).opnum();
            auto entry = msg.entries(i-firstEntryOpnum);
            //ASSERT(entry.state() == LOG_STATE_SPECULATIVE);
            ASSERT(entry.opnum() == i);

            pair<uint64_t, uint64_t> reqid(entry.request().clientid(),
                                           entry.request().clientreqid());
            if (seen.find(reqid) == seen.end()) {
                LogEntry newEntry;
                newEntry.viewstamp = viewstamp_t(newView,next);
                newEntry.state = LOG_STATE_SPECULATIVE;
                newEntry.request = entry.request();
                newEntry.hash = Log::ComputeHash(lastHash, newEntry);
                lastHash = newEntry.hash;
                out.push_back(newEntry);
                seen.insert(reqid);
                next++;
            }
        }
    }

    // Whew, we're done. If we're feeling paranoid, let's make sure
    // the output makes sense.
#if PARANOID
    // First sanity check: make sure the log is well-formed, i.e. the
    // opnums are in order with no gaps, the view numbers are
    // monotonically increasing, the hash chain is correct, and the
    // speculative entries come after the committed
    // entries. Furthermore, make sure that any speculative entries
    // are in our new view.
    opnum_t lastOpnum = 0;
    view_t lastView = 0;
    bool seenSpeculative = false;
    for (const auto &entry: out) {

        // Skip the checks for the first element, since we don't know
        // where the log starts.
        if (lastOpnum != 0) {
            ASSERT(entry.viewstamp.opnum == lastOpnum + 1);
            ASSERT(entry.viewstamp.view >= lastView);
            if (seenSpeculative) {
                ASSERT(entry.state == LOG_STATE_SPECULATIVE);
            }
            ASSERT(entry.hash == Log::ComputeHash(lastHash, entry));
        }

        lastOpnum = entry.viewstamp.opnum;
        lastView = entry.viewstamp.view;
        seenSpeculative = (entry.state == LOG_STATE_SPECULATIVE);
        lastHash = entry.hash;
    }

    // Make sure that anything that was committed in any of the old
    // logs is committed in the new log in the same place.
    for (auto &kv : dvcs) {
        for (auto &entry : kv.second.entries()) {
            if (entry.state() != LOG_STATE_COMMITTED) {
                continue;
            }

            opnum_t outStart = out[0].viewstamp.opnum;
            if (entry.opnum() < outStart) {
                // It might not be in the new log if the new log
                // doesn't start so early.
                continue;
            }
            LogEntry &newEntry = out[entry.opnum()-outStart];
            ASSERT(newEntry.viewstamp.view == entry.view());
            ASSERT(newEntry.viewstamp.opnum == entry.opnum());
            ASSERT(newEntry.request.op() == entry.request().op());
            ASSERT(newEntry.request.clientid() ==
                   entry.request().clientid());
            ASSERT(newEntry.request.clientreqid() ==
                   entry.request().clientreqid());
            ASSERT(newEntry.hash == entry.hash());
        }
    }

    // Make sure that everything that was speculative in any of the
    // new logs appears in the new log exactly once, anywhere. If the
    // old entry was committed, make sure its viewstamp matches.
    for (auto &kv : dvcs) {
        for (auto &entry : kv.second.entries()) {
            if (entry.opnum() < maxStart) {
                continue;
            }
            if (entry.state() != LOG_STATE_SPECULATIVE) {
                continue;
            }
            if (entry.view() != latestView) {
                continue;
            }

            bool found = false;
            for (const auto &newEntry : out) {
                if ((newEntry.request.clientid() ==
                     entry.request().clientid()) &&
                    (newEntry.request.clientreqid() ==
                     entry.request().clientreqid())) {
                    ASSERT(!found);
                    found = true;

                    ASSERT(newEntry.request.op() == entry.request().op());

                    if (entry.state() == LOG_STATE_COMMITTED) {
                        ASSERT(newEntry.viewstamp.view == entry.view());
                        ASSERT(newEntry.viewstamp.opnum == entry.opnum());
                    }
                }
            }
            ASSERT(found);
        }
    }
#endif

    RDebug("Merged log:");
    for (const auto &entry : out) {
        RDebug("  " FMT_VIEWSTAMP " (%lx, %lu) %s " FMT_BLOB,
               VA_VIEWSTAMP(entry.viewstamp),
               entry.request.clientid(), entry.request.clientreqid(),
               ((entry.state == LOG_STATE_COMMITTED) ? "committed" : "speculative"),
               VA_BLOB_STRING(entry.hash));
    }
    RDebug(" ");
    RNotice("Produced merged log with %zd entries", out.size());
}

void
SpecReplica::InstallLog(const std::vector<LogEntry> &entries)
{
    opnum_t newLastSpeculative, newLastCommitted, entriesStart;

    // Find the last committed and last speculative opnums in the
    // log.
    // XXX We might want to wrap up the log in something that contains
    // these already.
    if (entries.empty()) {
        newLastCommitted = 0;
        newLastSpeculative = 0;
        entriesStart = 0;
        ASSERT(lastCommitted == 0);
    } else {
        auto iter = entries.rbegin();
        newLastSpeculative = iter->viewstamp.opnum;
        while ((iter != entries.rend()) &&
               (iter->state == LOG_STATE_SPECULATIVE)) {
            iter++;
        }
        if (iter != entries.rend()) {
            newLastCommitted = iter->viewstamp.opnum;
        } else {
            newLastCommitted = 0;
        }

        // Make sure that we have enough log entries to install
        // without gaps
        // opnum_t firstOpnum = entries.begin()->viewstamp.opnum;
        // if (firstOpnum > lastCommitted + 1) {
        //     RPanic("Not enough entries in merged log to install without gaps;"
        //            " needed %ld got %ld",
        //            lastCommitted+1, firstOpnum);
        // }
        entriesStart = entries[0].viewstamp.opnum;
    }

    if (newLastCommitted < lastCommitted) {
        // This can happen in a corner case involving committing
        // through reconciliation and IN-VIEW messages. In that case,
        // it should be OK, because the new leader will propose
        // speculative operations that match our committed ones. In
        // that case, we can just pretend that newLastCommitted =
        // lastCommitted. But check that this is the case first...
        ASSERT(newLastSpeculative >= lastCommitted);
#if PARANOID
        for (opnum_t i = std::max(newLastCommitted+1, entriesStart);
             i <= lastCommitted; i++) {
            const LogEntry *oldEntry = log.Find(i);
            ASSERT(oldEntry != NULL);
            const LogEntry *newEntry = &entries[i-entriesStart];
            ASSERT(oldEntry->viewstamp.opnum == i);
            ASSERT(newEntry->viewstamp.opnum == i);
            ASSERT(oldEntry->viewstamp.view == newEntry->viewstamp.view);
            ASSERT(oldEntry->request.op() == newEntry->request.op());
            ASSERT(oldEntry->request.clientid() ==
                   newEntry->request.clientid());
            ASSERT(oldEntry->request.clientreqid() ==
                   newEntry->request.clientreqid());
            ASSERT(oldEntry->hash == newEntry->hash);
        }
#endif
        newLastCommitted = lastCommitted;
    }
    ASSERT(newLastCommitted >= lastCommitted);

#if PARANOID
    // Any operations we already have committed had better match the
    // log.
    if (!entries.empty()) {
        for (opnum_t i = entries.begin()->viewstamp.opnum;
             (i < lastCommitted) && (i < newLastCommitted);
             i++) {
            const LogEntry *oldEntry = log.Find(i);
            ASSERT(oldEntry != NULL);
            const LogEntry *newEntry = &entries[i-entriesStart];
            ASSERT(oldEntry->viewstamp.opnum == i);
            ASSERT(newEntry->viewstamp.opnum == i);
            ASSERT(oldEntry->viewstamp.view == newEntry->viewstamp.view);
            ASSERT(oldEntry->request.op() == newEntry->request.op());
            ASSERT(oldEntry->request.clientid() ==
                   newEntry->request.clientid());
            ASSERT(oldEntry->request.clientreqid() ==
                   newEntry->request.clientreqid());
            ASSERT(oldEntry->hash == newEntry->hash);
            ASSERT(oldEntry->state == LOG_STATE_COMMITTED);
            //ASSERT(newEntry->state == LOG_STATE_COMMITTED);
        }
    }
#endif

    // See if we can commit any of the speculative operations in our
    // log.
    opnum_t toCommit = std::max(lastCommitted+1, entriesStart);
    while ((toCommit <= newLastCommitted) &&
           (toCommit <= lastSpeculative)) {
        const LogEntry *oldEntry = log.Find(toCommit);
        ASSERT(oldEntry != NULL);
        const LogEntry *newEntry = &entries[toCommit-entriesStart];

        if (oldEntry->hash != newEntry->hash) {
            // Hashes don't match. These entries aren't the same, so
            // no point checking any further.
            break;
        }

        toCommit++;
    }
    if (toCommit > entriesStart) {
        toCommit--;
        CommitUpTo(toCommit);
        ASSERT(lastCommitted == toCommit);
    }
    ASSERT(newLastCommitted >= lastCommitted);

    // Now roll back any speculative operations that don't match the
    // log.
    opnum_t firstDiverging;
    for (firstDiverging = std::max(lastCommitted + 1, entriesStart);
         firstDiverging <= lastSpeculative;
         firstDiverging++) {
        if (firstDiverging > newLastSpeculative) {
            break;
        }

        const LogEntry *oldEntry = log.Find(firstDiverging);
        ASSERT(oldEntry != NULL);
        const LogEntry *newEntry = &entries[firstDiverging-entriesStart];

        if (oldEntry->hash != newEntry->hash) {
            break;
        }
    }
    RollbackTo(firstDiverging - 1);
    ASSERT(lastSpeculative < firstDiverging);

    // Now install any new speculative or committed operations
    for (opnum_t i = lastSpeculative + 1; i <= newLastSpeculative; i++) {
        SpeculativeReplyMessage reply;
        const LogEntry *newEntry = &entries[i-entriesStart];

        RDebug("Speculatively executing new operation (%lx, %ld) as " FMT_VIEWSTAMP,
                newEntry->request.clientid(),
                newEntry->request.clientreqid(),
                VA_VIEWSTAMP(newEntry->viewstamp));

        lastSpeculative++;
        LogEntry &installedEntry =
            log.Append(newEntry->viewstamp, newEntry->request,
                       LOG_STATE_SPECULATIVE);
        Execute(newEntry->viewstamp.opnum, newEntry->request, reply);

        // Prepare a reply to send to the client. Send it to the
        // client if we know the address. Either way, put it in the
        // client table so we have it available if the client
        // retries.
        reply.set_clientreqid(newEntry->request.clientreqid());
        reply.set_view(newEntry->viewstamp.view);
        reply.set_opnum(newEntry->viewstamp.opnum);
        reply.set_replicaidx(this->replicaIdx);
        reply.set_loghash(log.LastHash());
        reply.set_committed(newEntry->state == LOG_STATE_COMMITTED);

        auto addr = clientAddresses.find(newEntry->request.clientid());
        if (addr != clientAddresses.end()) {
            if (!(transport->SendMessage(this, *(addr->second), reply))) {
                RWarning("Failed to send speculative reply");
            }
        }
        UpdateClientTable(newEntry->request, installedEntry, reply);

        ASSERT(log.LastHash() == newEntry->hash);
    }

    ASSERT(lastSpeculative == newLastSpeculative);

    CommitUpTo(newLastCommitted);

    ASSERT(lastCommitted == newLastCommitted);
}

void
SpecReplica::SendFillDVCGapMessage(int replicaIdx,
                                   view_t view)
{
    ASSERT(replicaIdx != this->replicaIdx);
    Notice("Sending FillDVCGap " FMT_VIEWSTAMP " to %d",
           view, lastCommitted, replicaIdx);
    FillDVCGapMessage m;
    m.set_view(view);
    m.set_lastcommitted(lastCommitted);

    if (!(transport->SendMessageToReplica(this, replicaIdx, m))) {
        RWarning("Failed to send FillDVCGapMessage");
    }
}

void
SpecReplica::NeedFillDVCGap(view_t view)
{
    Warning("Need to fill DVC gap for " FMT_VIEW, view);
    DoViewChangeMessage mine;
    ASSERT(view == this->view);
    auto &dvcs = doViewChangeQuorum.GetMessages(view);
    std::vector<DoViewChangeMessage> ok;
    for (auto &kv : dvcs) {
        if (kv.first == this->replicaIdx) {
            mine = kv.second;
        } else {
            SendFillDVCGapMessage(kv.first, view);
        }
    }

    doViewChangeQuorum.Clear();
    doViewChangeQuorum.AddAndCheckForQuorum(view, this->replicaIdx, mine);

    needFillDVC = view;
}

void
SpecReplica::HandleDoViewChange(const TransportAddress &remote,
                                const DoViewChangeMessage &msg)
{
    RNotice("Received DOVIEWCHANGE " FMT_VIEW " from replica %d", msg.view(), msg.replicaidx());

    if (msg.view() < view) {
        RDebug("Ignoring DOVIEWCHANGE for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RDebug("Ignoring DOVIEWCHANGE for current view");
        return;
    }

    if ((status != STATUS_VIEW_CHANGE) || (msg.view() > view)) {
        // It's superfluous to send the StartViewChange messages here,
        // but harmless...
        StartViewChange(msg.view());
    }

    ASSERT(configuration.GetLeaderIndex(msg.view()) == this->replicaIdx);


    opnum_t maxStart = 0;
    if (needFillDVC == msg.view()) {
        if (msg.entries_size() > 0) {
            opnum_t firstEntryOpnum = msg.entries(0).opnum();
            if (!(lastCommitted == 0 && firstEntryOpnum == 1) &&
                ((firstEntryOpnum > lastCommitted) ||
                 (firstEntryOpnum > msg.lastcommitted()))) {
                SendFillDVCGapMessage(msg.replicaidx(), msg.view());
                return;
            }
        }
        doViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                msg.replicaidx(), msg);
    } else {
        doViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                msg.replicaidx(), msg);

        auto &dvcs = doViewChangeQuorum.GetMessages(msg.view());
        if (dvcs.size() > 0) {
            for (auto &kv : dvcs) {
                if (kv.second.entries_size() > 0) {
                    opnum_t start = kv.second.entries(0).opnum();
                    if (start > maxStart) {
                        maxStart = start;
                    }
                }
            }
            if (maxStart > lastSpeculative) {
                NeedFillDVCGap(msg.view());
                return;
            }
            if (maxStart > 0) {
                Notice("maxStart %ld lastSpeculative %ld", maxStart, lastSpeculative);
                const LogEntry *myEntry = log.Find(maxStart);
                ASSERT(myEntry != NULL);
                for (auto &kv : dvcs) {
                    if (kv.second.entries_size() > 0) {
                        if (kv.second.lastspeculative() < maxStart) {
                            Notice("Log from %d starts at %ld; need to fill gap", kv.first, kv.second.lastspeculative());
                            NeedFillDVCGap(msg.view());
                            return;
                        }
                        opnum_t entriesStart = kv.second.entries(0).opnum();
                        ASSERT((unsigned long)maxStart-entriesStart < (unsigned long)kv.second.entries_size());
                        auto &entry = kv.second.entries(maxStart-entriesStart);
                        ASSERT(entry.opnum() == maxStart);
                        if (entry.hash() != myEntry->hash) {
                            NeedFillDVCGap(msg.view());
                            return;
                        } else {
                            Notice("%d entry %ld matches", kv.first, entry.opnum());
                        }
                    }
                }
            }
        }
    }


    auto msgs = doViewChangeQuorum.CheckForQuorum(msg.view());
    if (msgs != NULL) {
        // We have a quorum of DOVIEWCHANGE messages. Now it's time to
        // merge the logs.
        std::vector<LogEntry> newLog;
        Latency_Start(&mergeLatency);
        MergeLogs(view, maxStart, *msgs, newLog);
        Latency_End(&mergeLatency);

        // Install the newly merged log.
        InstallLog(newLog);

        // How much of the log should we include when we send the
        // STARTVIEW message? Start from the lowest committed opnum of
        // any of the STARTVIEWCHANGE or DOVIEWCHANGE messages we got.
        //
        // We need to compute this before we enter the new view
        // because the saved messages will go away.
        auto svcs = startViewChangeQuorum.GetMessages(view);
        opnum_t minCommittedSVC = std::min_element(
            svcs.begin(), svcs.end(),
            [](decltype(*svcs.begin()) a,
               decltype(*svcs.begin()) b) {
                return a.second.lastcommitted() < b.second.lastcommitted();
            })->second.lastcommitted();
        opnum_t minCommittedDVC = std::min_element(
            msgs->begin(), msgs->end(),
            [](decltype(*msgs->begin()) a,
               decltype(*msgs->begin()) b) {
                return a.second.lastcommitted() < b.second.lastcommitted();
            })->second.lastcommitted();
        opnum_t minCommitted = std::min(minCommittedSVC, minCommittedDVC);
        // This might not be necessary, since we should have our own
        // DOVIEWCHANGE message, but just in case...
        minCommitted = std::min(minCommitted, lastCommitted);
        if (minCommitted > LOG_FUDGE) {
            minCommitted -= LOG_FUDGE;
        } else {
            minCommitted = 0;
        }

        // XXX And then do something totally different instead...
        // which I sadly do not have time to explain here
        if (lastSpeculative > DVC_FUDGE) {
            minCommitted = lastSpeculative - DVC_FUDGE;
        } else {
            minCommitted = 1;
        }

        // Start the new view
        EnterView(msg.view());
        ASSERT(AmLeader());;

        // Send the log to the other replicas
        StartViewMessage sv;
        sv.set_view(view);
        sv.set_lastspeculative(lastSpeculative);
        sv.set_lastcommitted(lastCommitted);

        log.Dump(minCommitted, sv.mutable_entries());

        if (!(transport->SendMessageToAll(this, sv))) {
            RWarning("Failed to send StartView message to all replicas");
        }
    }
}

template<class iter>
static void
ConvertLog(iter start, iter end,
           std::vector<LogEntry> &out)
{
    opnum_t last = 0;
    for (iter it = start; it != end; it++) {
        LogEntry e;
        e.viewstamp.view = it->view();
        e.viewstamp.opnum = it->opnum();
        e.request = it->request();
        e.state = (LogEntryState) it->state();
        e.hash = it->hash();
        out.push_back(e);
        if (last != 0) {
            ASSERT(last+1 == it->opnum());
        }
        last = it->opnum();
    }
}

void
SpecReplica::HandleStartView(const TransportAddress &remote,
                             const StartViewMessage &msg)
{
    RDebug("Received STARTVIEW " FMT_VIEW, msg.view());

    if (msg.view() < view) {
        RWarning("Ignoring STARTVIEW for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RWarning("Ignoring STARTVIEW for current view");
        return;
    }

    ASSERT(configuration.GetLeaderIndex(msg.view()) != this->replicaIdx);

    // Make sure we have enough entries
    if ((msg.entries_size() != 0) &&
        (msg.entries(0).opnum() > lastCommitted + 1)) {
        // See if we can make do with what we've got
        if ((msg.entries(0).opnum() > lastSpeculative) ||
            (msg.entries(0).hash() !=
             log.Find(msg.entries(0).opnum())->hash)) {
            Notice("Requesting longer log");
            FillLogGapMessage flg;
            flg.set_view(msg.view());
            flg.set_lastcommitted(lastCommitted);
            if (!(transport->SendMessage(this, remote, flg))) {
                RWarning("Failed to send FillLogGapMessage");
            }
            return;
        }
    }

    // Install the new log
    std::vector<LogEntry> newLog;
    ConvertLog(msg.entries().begin(), msg.entries().end(),
               newLog);
    InstallLog(newLog);
    CommitUpTo(msg.lastcommitted());

    // Send in-view message
    InViewMessage iv;
    iv.set_replicaidx(this->replicaIdx);
    iv.set_view(msg.view());
    iv.set_lastspeculative(msg.lastspeculative());
    int leader = configuration.GetLeaderIndex(msg.view());
    if (!(transport->SendMessageToReplica(this, leader, iv))) {
        RWarning("Failed to send InView message to leader of new view");
    }

    // Note that these will no longer necessarily be true after we
    // EnterView because that may process some deferred requests.
    ASSERT_EQ(lastSpeculative, msg.lastspeculative());
    ASSERT_GE(lastCommitted, msg.lastcommitted());

    // Start new view
    EnterView(msg.view());
    ASSERT(!AmLeader());
}


void
SpecReplica::HandleInView(const TransportAddress &remote,
                          const InViewMessage &msg)
{
    RDebug("Received INVIEW " FMT_VIEW, msg.view());

    if (msg.view() < view) {
        RWarning("Ignoring INVIEW for older view");
        return;
    }

    if ((msg.view() > view) || (status != STATUS_NORMAL)) {
        RPanic("Received INVIEW message for later view");
    }

    ASSERT(configuration.GetLeaderIndex(msg.view()) == this->replicaIdx);

    if (inViewQuorum.AddAndCheckForQuorum(msg.view(), msg.replicaidx(),
                                          msg) != NULL) {
        ASSERT(msg.lastspeculative() <= lastSpeculative);
        if (msg.lastspeculative() < lastCommitted) {
            return;
        }
        RNotice("Have quorum of INVIEW messages; committing up to %ld",
                msg.lastspeculative());
        CommitUpTo(msg.lastspeculative());
        SendSync();
    }
}

void
SpecReplica::HandleFillLogGap(const TransportAddress &remote,
                              const FillLogGapMessage &msg)
{
    RNotice("Received FILLLOGGAP " FMT_VIEW, msg.view());

    if (msg.view() != view) {
        RNotice("Ignoring FillLogGap message; wrong view");
        return;
    }

    ASSERT(AmLeader());

    StartViewMessage sv;
    sv.set_view(view);
    sv.set_lastspeculative(lastSpeculative);
    sv.set_lastcommitted(lastCommitted);
    log.Dump(msg.lastcommitted(), sv.mutable_entries());

    if (!(transport->SendMessage(this, remote, sv))) {
        RWarning("Failed to send StartView message to requesting replica");
    }
}

void
SpecReplica::HandleFillDVCGap(const TransportAddress &remote,
                              const FillDVCGapMessage &msg)
{
    RNotice("Received FILLDVCGAP " FMT_VIEW, msg.view());

    if (msg.view() != view) {
        RNotice("Ignoring FillDVCGap message; wrong view");
        return;
    }

    ASSERT(!AmLeader());

    DoViewChangeMessage dvc;
    dvc.set_view(view);
    dvc.set_lastnormalview(log.LastViewstamp().view);
    dvc.set_lastspeculative(lastSpeculative);
    dvc.set_lastcommitted(lastCommitted);
    dvc.set_replicaidx(this->replicaIdx);

    opnum_t x = std::min(lastCommitted, msg.lastcommitted());
    log.Dump(x, dvc.mutable_entries());

    if (!(transport->SendMessage(this, remote, dvc))) {
        RWarning("Failed to send DoViewChangeMessage message to requesting replica");
    }
}


} // namespace specpaxos::spec

} // namespace specpaxos
