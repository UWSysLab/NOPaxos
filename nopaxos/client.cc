// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * nopaxos/client.cc:
 *   Network Ordered Paxos Client
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

#include "nopaxos/client.h"
#include "lib/assert.h"
#include "lib/message.h"

namespace specpaxos {
namespace nopaxos {

NOPaxosClient::NOPaxosClient(const Configuration &config,
                             Transport *transport,
                             uint64_t clientid)
    : Client(config, transport, clientid),
    replyQuorum(config.QuorumSize())
{
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;
    lastReqID = 0;

    requestTimeout = new Timeout(transport, 100, [this]() {
        ResendRequest();
    });
    unloggedRequestTimeout = new Timeout(transport, 100, [this]() {
        UnloggedRequestTimeoutCallback();
    });

}

NOPaxosClient::~NOPaxosClient()
{
    if (pendingRequest) {
        delete pendingRequest;
    }
    if (pendingUnloggedRequest) {
        delete pendingUnloggedRequest;
    }
}

void
NOPaxosClient::Invoke(const string &request,
                      continuation_t continuation)
{
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqID;
    pendingRequest = new PendingRequest(request, lastReqID, continuation);

    SendRequest();
}

void
NOPaxosClient::InvokeUnlogged(int replicaIdx,
                              const string &request,
                              continuation_t continuation,
                              timeout_continuation_t timeoutContinuation,
                              uint32_t timeout)
{
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqID;
    pendingUnloggedRequest = new PendingRequest(request, lastReqID, continuation);
    pendingUnloggedRequest->timeoutContinuation = timeoutContinuation;

    proto::UnloggedRequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(lastReqID);

    ASSERT(!unloggedRequestTimeout->Active());
    unloggedRequestTimeout->SetTimeout(timeout);
    unloggedRequestTimeout->Start();

    transport->SendMessageToReplica(this, replicaIdx, reqMsg);
}

void
NOPaxosClient::SendRequest()
{
    proto::RequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingRequest->clientReqID);
    reqMsg.set_msgnum(0);
    reqMsg.set_sessnum(0);

    transport->OrderedMulticast(this, reqMsg);

    requestTimeout->Reset();
}

void
NOPaxosClient::ResendRequest()
{
    Warning("Client timeout; resending request");
    SendRequest();
}

void
NOPaxosClient::ReceiveMessage(const TransportAddress &remote,
                              const string &type,
                              const string &data,
                              void *meta_data)
{
    static proto::ReplyMessage reply;
    static proto::UnloggedReplyMessage unloggedReply;

    if (type == reply.GetTypeName()) {
        reply.ParseFromString(data);
        HandleReply(remote, reply);
    } else if (type == unloggedReply.GetTypeName()) {
        unloggedReply.ParseFromString(data);
        HandleUnloggedReply(remote, unloggedReply);
    } else {
        Client::ReceiveMessage(remote, type, data, nullptr);
    }
}

void
NOPaxosClient::CompleteOperation(const proto::ReplyMessage &msg)
{
    ASSERT(msg.has_reply());
    requestTimeout->Stop();

    PendingRequest *req = pendingRequest;
    pendingRequest = NULL;

    req->continuation(req->request, msg.reply());
    replyQuorum.Clear();

    delete req;
}

void
NOPaxosClient::HandleReply(const TransportAddress &remote,
                           const proto::ReplyMessage &msg)
{
    if (pendingRequest == NULL) {
        return;
    }
    if (msg.clientreqid() != pendingRequest->clientReqID) {
        return;
    }

    if (auto msgs = replyQuorum.AddAndCheckForQuorum(msg.clientreqid(),
                                                     msg.replicaidx(),
                                                     msg)) {
        bool hasLeader = false;
        view_t latestView = 0;
        sessnum_t latestSessnum = 0;
        int leaderIdx;
        int matching = 0;

        /* Find the leader reply in the latest view */
        for (auto &kv : *msgs) {
            // If found higher view/session, previous
            // leader message was in an older view.
            if (kv.second.view() > latestView) {
                latestView = kv.second.view();
                hasLeader = false;
            }
            if (kv.second.sessnum() > latestSessnum) {
                latestSessnum = kv.second.sessnum();
            }

            if (IsLeader(kv.second.view(), kv.first) &&
                kv.second.view() == latestView &&
                kv.second.sessnum() == latestSessnum) {
                hasLeader = true;
                leaderIdx = kv.first;
            }
        }

        if (hasLeader) {
            /* Do we having matching replies? */
            const proto::ReplyMessage &leaderMessage = msgs->at(leaderIdx);

            for (auto &kv : *msgs) {
                if (kv.second.view() == leaderMessage.view() &&
                    kv.second.sessnum() == leaderMessage.sessnum() &&
                    kv.second.opnum() == leaderMessage.opnum()) {
                    ASSERT(kv.second.clientreqid() == leaderMessage.clientreqid());
                    matching++;
                }
            }

            if (matching >= config.QuorumSize()) {
                CompleteOperation(leaderMessage);
            }
        }
    }
}

void
NOPaxosClient::HandleUnloggedReply(const TransportAddress &remote,
                                   const proto::UnloggedReplyMessage &msg)
{
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unloggedReply when no request was pending");
        return;
    }

    unloggedRequestTimeout->Stop();

    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    req->continuation(req->request, msg.reply());
    delete req;
}

void
NOPaxosClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();

    req->timeoutContinuation(req->request);
}

bool
NOPaxosClient::IsLeader(view_t view, int replicaIdx)
{
    return (config.GetLeaderIndex(view) == replicaIdx);
}

} // namespace specpaxos::nopaxos
} // namespace specpaxos
