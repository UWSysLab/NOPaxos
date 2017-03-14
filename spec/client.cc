// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * spec/client.cc:
 *   Speculative Paxos client
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

#include "common/client.h"
#include "common/request.pb.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "spec/client.h"
#include "spec/spec-proto.pb.h"

namespace specpaxos {
namespace spec {

using namespace specpaxos::spec::proto;

SpecClient::SpecClient(const Configuration &config,
                       Transport *transport,
                       uint64_t clientid)
    : Client(config, transport, clientid),
      speculativeReplyQuorum(config.FastQuorumSize())
{
    lastReqId = 0;
    view = 0;
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;

    requestTimeout = new Timeout(transport, 7000, [this]() {
            Warning("Client timed out; resending request");
            ResendRequest();
        });
    unloggedRequestTimeout = new Timeout(transport, 1000, [this]() {
            UnloggedRequestTimeoutCallback();
        });
}

SpecClient::~SpecClient()
{
    if (pendingRequest) {
        delete pendingRequest;
    }
    if (pendingUnloggedRequest) {
        delete pendingUnloggedRequest;
    }
    delete requestTimeout;
    delete unloggedRequestTimeout;
}

void
SpecClient::Invoke(const string &request,
                   continuation_t continuation)
{
    // XXX Can only handle one pending request for now
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;
    pendingRequest = new PendingRequest(request, reqId, continuation);

    SendRequest();
}

void
SpecClient::InvokeUnlogged(int replicaIdx,
                         const string &request,
                         continuation_t continuation,
                         timeout_continuation_t timeoutContinuation,
                         uint32_t timeout)
{
    // XXX Can only handle one pending request for now
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;

    pendingUnloggedRequest = new PendingRequest(request, reqId, continuation);
    pendingUnloggedRequest->timeoutContinuation = timeoutContinuation;

    proto::UnloggedRequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingUnloggedRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingUnloggedRequest->clientReqId);

    ASSERT(!unloggedRequestTimeout->Active());
    unloggedRequestTimeout->SetTimeout(timeout);
    unloggedRequestTimeout->Start();

    transport->SendMessageToReplica(this, replicaIdx, reqMsg);
}


void
SpecClient::SendRequest()
{
    RequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingRequest->clientReqId);

    transport->SendMessageToAll(this, reqMsg);

    requestTimeout->Reset();
}

void
SpecClient::ResendRequest()
{
    SendRequest();
}


void
SpecClient::ReceiveMessage(const TransportAddress &remote,
                           const string &type,
                           const string &data,
                           void *meta_data)
{
    static RequestMessage reqMsg;
    static SpeculativeReplyMessage reply;
    static UnloggedReplyMessage unloggedReply;

    if (type == reqMsg.GetTypeName()) {
        // Ignore
        return;
    } else if (type == reply.GetTypeName()) {
        reply.ParseFromString(data);
        HandleReply(remote, reply);
    } else if (type == unloggedReply.GetTypeName()) {
        unloggedReply.ParseFromString(data);
        HandleUnloggedReply(remote, unloggedReply);
    } else {
        Client::ReceiveMessage(remote, type, data, meta_data);
    }
}

void
SpecClient::CompleteOperation(const SpeculativeReplyMessage &msg)
{
    // Now we've got n-e matching responses. We can consider
    // the operation complete.
    requestTimeout->Stop();

    PendingRequest *req = pendingRequest;
    pendingRequest = NULL;
    speculativeReplyQuorum.Clear();

    Debug("Completed operation %ld", req->clientReqId);
    req->continuation(req->request, msg.reply());

    delete req;
}

void
SpecClient::HandleReply(const TransportAddress &remote,
                        const SpeculativeReplyMessage &msg)
{
    if (pendingRequest == NULL) {
        Debug("Received reply when no request was pending");
        return;
    }

    if (msg.clientreqid() != pendingRequest->clientReqId) {
        Debug("Received reply for a different request");
        return;
    }

    Debug("Client received %s reply from replica %d",
          msg.committed() ? "non-speculative" : "speculative",
          msg.replicaidx());

    if (view < msg.view()) {
        Notice("New view: %ld", msg.view());
        speculativeReplyQuorum.Clear();
        view = msg.view();
    }

    if (msg.committed()) {
        CompleteOperation(msg);
        return;
    }

    if (auto msgs =
        speculativeReplyQuorum.AddAndCheckForQuorum(msg.clientreqid(),
                                                    msg.replicaidx(),
                                                    msg)) {
        /*
         * We now have a quorum of at least n-e responses. Do they
         * match?
         */
        int matching = 0;

        for (auto &kv : *msgs) {
            if (kv.second.loghash() == msg.loghash()) {
                matching++;
                ASSERT(kv.second.clientreqid() == msg.clientreqid());
                ASSERT(kv.second.view() == msg.view());
                ASSERT(kv.second.opnum() == msg.opnum());
                ASSERT(kv.second.reply() == msg.reply());
            }
        }

	if (matching >= config.FastQuorumSize()) {
	    CompleteOperation(msg);
	} else {
	    // XXX This gets triggered if there are n-e responses and
	    // they don't all match.
	    Warning("Non-matching quorum in view %ld; requesting view change",
		    msg.view());
	    for (auto &kv : *msgs) {
		Warning("  replica %d: " FMT_VIEWSTAMP, kv.first,
			kv.second.view(), kv.second.opnum());
	    }

	    RequestViewChangeMessage rvc;
	    rvc.set_view(msg.view());
	    transport->SendMessageToAll(this, rvc);
	}
    }
}

void
SpecClient::HandleUnloggedReply(const TransportAddress &remote,
                              const proto::UnloggedReplyMessage &msg)
{
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unloggedReply when no request was pending");
        return;
    }

    Debug("Client received unloggedReply");

    unloggedRequestTimeout->Stop();

    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    req->continuation(req->request, msg.reply());
    delete req;
}

void
SpecClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();

    req->timeoutContinuation(req->request);
}

} // namespace spec
} // namespace specpaxos
