// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * unreplicated/client.h:
 *   dummy implementation of replication interface that just uses a
 *   single replica and passes commands directly to it
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

#ifndef _UNREPLICATED_CLIENT_H_
#define _UNREPLICATED_CLIENT_H_

#include "common/client.h"
#include "lib/configuration.h"
#include "unreplicated/unreplicated-proto.pb.h"

namespace specpaxos {
namespace unreplicated {

class UnreplicatedClient : public Client
{
public:
    UnreplicatedClient(const Configuration &config,
                       Transport *transport,
                       uint64_t clientid = 0);
    virtual ~UnreplicatedClient();
    virtual void Invoke(const string &request, continuation_t continuation) override;
    virtual void InvokeUnlogged(int replicaIdx,
                                const string &request,
                                continuation_t continuation,
                                timeout_continuation_t timeoutContinuation = nullptr,
                                uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
    virtual void ReceiveMessage(const TransportAddress &remote,
                        const string &type, const string &data,
                        void *meta_data) override;

protected:
    struct PendingRequest
    {
        string request;
	uint64_t clientid;
	uint64_t clientreqid;
        continuation_t continuation;
        inline PendingRequest(string request, uint64_t clientreqid, continuation_t continuation)
            : request(request), clientreqid(clientreqid), continuation(continuation) { }
    };
    PendingRequest *pendingRequest;
    PendingRequest *pendingUnloggedRequest;
    Timeout *requestTimeout;
    uint64_t lastReqId;

    void HandleReply(const TransportAddress &remote,
                     const proto::ReplyMessage &msg);
    void HandleUnloggedReply(const TransportAddress &remote,
                             const proto::UnloggedReplyMessage &msg);
    void SendRequest();
    void ResendRequest();
};

} // namespace specpaxos::unreplicated
} // namespace specpaxos

#endif  /* _UNREPLICATED_CLIENT_H_ */
