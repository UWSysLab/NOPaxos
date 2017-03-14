// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * fastpaxos/client.h:
 *   Fast Paxos client
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

#ifndef _FASTPAXOS_CLIENT_H_
#define _FASTPAXOS_CLIENT_H_

#include "common/client.h"
#include "lib/configuration.h"
#include "fastpaxos/fastpaxos-proto.pb.h"

namespace specpaxos {
namespace fastpaxos {

class FastPaxosClient : public Client
{
public:
    FastPaxosClient(const Configuration &config,
                    Transport *transport,
                    uint64_t clientid = 0);
    virtual ~FastPaxosClient();
    virtual void Invoke(const string &request,
                        continuation_t continuation);
    virtual void InvokeUnlogged(int replicaIdx,
                                const string &request,
                                continuation_t continuation,
                                timeout_continuation_t timeoutContinuation = nullptr,
                                uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT);
    virtual void ReceiveMessage(const TransportAddress &remote,
                                const string &type, const string &data,
                                void *meta_data);

protected:
    int view;
    int opnumber;
    uint64_t lastReqId;

    struct PendingRequest
    {
        string request;
        uint64_t clientReqId;
        continuation_t continuation;
        timeout_continuation_t timeoutContinuation;
        inline PendingRequest(string request, uint64_t clientReqId,
                              continuation_t continuation)
            : request(request), clientReqId(clientReqId),
              continuation(continuation) { }
    };
    PendingRequest *pendingRequest;
    PendingRequest *pendingUnloggedRequest;
    Timeout *requestTimeout;
    Timeout *unloggedRequestTimeout;

    void SendRequest();
    void ResendRequest();
    void HandleReply(const TransportAddress &remote,
                     const proto::ReplyMessage &msg);
    void HandleUnloggedReply(const TransportAddress &remote,
                             const proto::UnloggedReplyMessage &msg);
    void UnloggedRequestTimeoutCallback();
};

} // namespace specpaxos::fastpaxos
} // namespace specpaxos

#endif  /* _FASTPAXOS_CLIENT_H_ */
