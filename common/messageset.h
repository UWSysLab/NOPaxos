// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * messageset.h:
 *   utility type for tracking sets of messages received
 *
 * Copyright 2016 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                Jialin Li  <lijl@cs.washington.edu>
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

#ifndef _COMMON_MESSAGESET_H_
#define _COMMON_MESSAGESET_H_

#include <map>
#include <vector>

namespace specpaxos {

template <class IDTYPE, class MSGTYPE>
class MessageSet
{
public:
    MessageSet(uint32_t replicaRequired)
        : replicaRequired(replicaRequired)
    {

    }

    void
    Clear()
    {
        messages.clear();
    }

    void
    Clear(IDTYPE vs)
    {
        if (messages.find(vs) == messages.end()) {
            return;
        }
        std::map<int, std::map<int, MSGTYPE> > &vsmessages = messages[vs].first;
        vsmessages.clear();
    }

    void
    Remove(IDTYPE vs)
    {
        messages.erase(vs);
    }

    int
    ReplicaRequied() const
    {
        return replicaRequired;
    }

    const std::map<int, std::map<int, MSGTYPE> > *
    GetMessages(IDTYPE vs)
    {
        if (messages.find(vs) == messages.end()) {
            return NULL;
        }
        return &(messages[vs].first);
    }

    const std::map<int, std::map<int, MSGTYPE> > *
    CheckForQuorum(IDTYPE vs)
    {
        if (messages.find(vs) == messages.end()) {
            return NULL;
        }

        std::map<int, std::map<int, MSGTYPE> > &vsmessages = messages[vs].first;
        if (vsmessages.size() >= messages[vs].second) {
            for (auto &replicas : vsmessages) {
                if (replicas.second.size() < replicaRequired) {
                    return NULL;
                }
            }
            return &vsmessages;
        } else {
            return NULL;
        }
    }

    const std::map<int, std::map<int, MSGTYPE> > *
    AddAndCheckForQuorum(IDTYPE vs, int shard,
                         int replica, const MSGTYPE &msg)
    {
        if (messages.find(vs) == messages.end()) {
            return NULL;
        }

        std::map<int, std::map<int, MSGTYPE> > &vsmessages = messages[vs].first;

        (vsmessages[shard])[replica] = msg;

        return CheckForQuorum(vs);
    }

    void
    Add(IDTYPE vs, int shard, int replica, const MSGTYPE &msg)
    {
        AddAndCheckForQuorum(vs, shard, replica, msg);
    }

    void
    SetShardRequired(IDTYPE vs, uint32_t num)
    {
        messages[vs].second = num;
    }

public:
    uint32_t replicaRequired;
private:
    std::map<IDTYPE, std::pair<std::map<int, std::map<int, MSGTYPE> >, uint32_t> > messages;
};

}      // namespace specpaxos

#endif  // _COMMON_MESSAGESET_H_
