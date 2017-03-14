// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * simtransport.h:
 *   simulated message-passing interface for testing use
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *        Jialin Li    <lijl@cs.washington.edu>
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

#ifndef _LIB_SIMTRANSPORT_H_
#define _LIB_SIMTRANSPORT_H_

#include "lib/transport.h"
#include "lib/transportcommon.h"

#include <deque>
#include <map>
#include <functional>

class SimulatedTransportAddress : public TransportAddress
{
public:
    SimulatedTransportAddress * clone() const;
    int GetAddr() const;
    bool operator==(const SimulatedTransportAddress &other) const;
    inline bool operator!=(const SimulatedTransportAddress &other) const
    {
        return !(*this == other);
    }
private:
    SimulatedTransportAddress(int addr);

    int addr;
    friend class SimulatedTransport;
};

class SimulatedTransport :
    public TransportCommon<SimulatedTransportAddress>
{
    typedef std::function<bool (TransportReceiver*, std::pair<int, int>,
                                TransportReceiver*, std::pair<int, int>,
                                Message &, uint64_t &delay)> filter_t;
public:
    SimulatedTransport(bool continuous = false);
    ~SimulatedTransport();
    void Register(TransportReceiver *receiver,
                  const specpaxos::Configuration &config,
                  int groupIdx,
                  int replicaIdx) override;
    void Run() override;
    void Stop() override;
    void AddFilter(int id, filter_t filter);
    void RemoveFilter(int id);
    int Timer(uint64_t ms, timer_callback_t cb) override;
    bool CancelTimer(int id) override;
    void CancelAllTimers() override;
    bool OrderedMulticast(TransportReceiver *src,
                          const std::vector<int> &groups,
                          const Message &m) override;

    // Returns if simtransport still have timers
    bool HasTimers() {
        return !timers.empty();
    }

    // Increment session number. (for testing only)
    void SessionChange();

protected:
    bool SendMessageInternal(TransportReceiver *src,
                             const SimulatedTransportAddress &dstAddr,
                             const Message &m) override;
    SimulatedTransportAddress
    LookupAddress(const specpaxos::Configuration &cfg,
                  int groupdIdx,
                  int replicaIdx) override;
    const SimulatedTransportAddress *
    LookupMulticastAddress(const specpaxos::Configuration *cfg) override;
    const SimulatedTransportAddress *
    LookupFCAddress(const specpaxos::Configuration *cfg) override;

private:
    struct QueuedMessage {
        int dst;
        int src;
        string type;
        string msg;
        multistamp_t stamp;
        inline QueuedMessage(int dst, int src,
                             const string &type, const string &msg, const multistamp_t &stamp) :
            dst(dst), src(src), type(type), msg(msg), stamp(stamp) { }
    };
    struct PendingTimer {
        uint64_t when;
        int id;
        timer_callback_t cb;
    };

    std::deque<QueuedMessage> queue;
    std::map<int, TransportReceiver *> endpoints;
    int lastAddr;
    std::map<int, std::pair<int, int> > replicaIdxs; // address to <groupIdx, replicaIdx>
    int fcAddress;
    std::multimap<int, filter_t> filters;
    std::multimap<uint64_t, PendingTimer> timers;
    int lastTimerId;
    uint64_t vtime;
    bool processTimers;
    bool running;
    bool continuous;

    // Network-Ordered packet counters
    std::map<int, uint64_t> noCounters; // (64bit) counter for each (32bit) group
    uint64_t sequencerID; // current sequencer ID

    bool _SendMessageInternal(TransportReceiver *src,
                              const SimulatedTransportAddress &dstAddr,
                              const Message &m,
                              const multistamp_t &stamp);
    void RegisterEndpoint(TransportReceiver *receiver, int groupIdx, int replicaIdx);
    void RegisterFC();
};

#endif  // _LIB_SIMTRANSPORT_H_
