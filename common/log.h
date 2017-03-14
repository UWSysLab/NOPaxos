// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * log.h:
 *   a replica's log of pending and committed operations
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

#ifndef _COMMON_LOG_H_
#define _COMMON_LOG_H_

#include "common/request.pb.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/viewstamp.h"

#include <map>
#include <google/protobuf/message.h>

namespace specpaxos {

enum LogEntryState {
    LOG_STATE_COMMITTED,
    LOG_STATE_PREPARED,
    LOG_STATE_SPECULATIVE,      // specpaxos only
    LOG_STATE_FASTPREPARED,     // fastpaxos only
    LOG_STATE_RECEIVED,		// nopaxos only
    LOG_STATE_NOOP,		// nopaxos only
    LOG_STATE_EXECUTED          // granola
};


class Log
{

public:
    struct LogEntry
    {
        viewstamp_t viewstamp;
        LogEntryState state;
        Request request;
        string hash;
        // Speculative client table stuff
        opnum_t prevClientReqOpnum;
        ::google::protobuf::Message *replyMessage;
        // Other data defined by protocols
        void *data;
        size_t data_len;

        LogEntry() { replyMessage = NULL; data = NULL; data_len = 0;}
        LogEntry(const LogEntry &x)
            : viewstamp(x.viewstamp), state(x.state), request(x.request),
              hash(x.hash), prevClientReqOpnum(x.prevClientReqOpnum)
            {
                if (x.replyMessage) {
                    replyMessage = x.replyMessage->New();
                    replyMessage->CopyFrom(*x.replyMessage);
                } else {
                    replyMessage = NULL;
                }

                if (x.data && x.data_len > 0) {
                    data = malloc(x.data_len);
                    data_len = x.data_len;
                    memcpy(data, x.data, x.data_len);
                } else {
                    data = NULL;
                    data_len = 0;
                }
            }
        LogEntry(viewstamp_t viewstamp,
                 LogEntryState state,
                 const Request &request,
                 const string &hash=Log::EMPTY_HASH)
            : viewstamp(viewstamp), state(state), request(request),
              hash(hash), replyMessage(NULL), data(NULL), data_len(0) { }
        virtual ~LogEntry()
            {
                if (replyMessage) {
                    delete replyMessage;
                }
                if (data) {
                    free(data);
                }
            }
    };

    Log(bool useHash, opnum_t start = 1, string initialHash = EMPTY_HASH);
    LogEntry & Append(viewstamp_t vs, const Request &req, LogEntryState state,
                      void *data = NULL, size_t data_len = 0);
    LogEntry & Append(viewstamp_t vs,
                      const Request &req,
                      const std::set<viewstamp_t> &vss,
                      LogEntryState state,
                      void *data = NULL,
                      size_t data_len = 0);
    LogEntry * Find(opnum_t opnum);
    LogEntry * Find(viewstamp_t vs);
    LogEntry * Find(const std::pair<uint64_t, uint64_t> &reqid);
    bool SetStatus(opnum_t opnum, LogEntryState state);
    bool SetRequest(opnum_t op, const Request &req);
    void RemoveAfter(opnum_t opnum);
    LogEntry * Last();
    viewstamp_t LastViewstamp() const; // deprecated
    opnum_t LastOpnum() const;
    opnum_t FirstOpnum() const;
    bool Empty() const;
    template <class T> void Dump(opnum_t from, T out);
    template <class T> void Dump(opnum_t from, opnum_t to, T out);
    template <class iter> void Install(iter start, iter end);
    const string &LastHash() const;

    static string ComputeHash(string lastHash, const LogEntry &entry);
    static const string EMPTY_HASH;


private:
    std::vector<LogEntry> entries;
    string initialHash;
    opnum_t start;
    bool useHash;

    // Eris: Search log entry using viewstamps (from other shards)
    std::map<viewstamp_t, opnum_t> vssMap;
    // Granola: Search log entry using <clientid, clientreqid> tuple
    std::map<std::pair<uint64_t, uint64_t>, opnum_t> clientReqMap;
};

typedef Log::LogEntry LogEntry;

#include "common/log-impl.h"

}      // namespace specpaxos

#endif  /* _COMMON_LOG_H_ */
