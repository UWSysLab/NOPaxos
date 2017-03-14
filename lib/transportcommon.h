// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * transport-common.h:
 *   template support for implementing transports
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                Jialin Li        <lijl@cs.washington.edu>
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

#ifndef _LIB_TRANSPORTCOMMON_H_
#define _LIB_TRANSPORTCOMMON_H_

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/transport.h"

#include <map>
#include <unordered_map>

template <typename ADDR>
class TransportCommon : public Transport
{

public:
    TransportCommon()
    {
        replicaAddressesInitialized = false;
    }

    virtual
    ~TransportCommon()
    {
        for (auto &kv : canonicalConfigs) {
            delete kv.second;
        }
    }

    virtual bool
    SendMessage(TransportReceiver *src, const TransportAddress &dst,
                const Message &m) override
    {
        const ADDR &dstAddr = dynamic_cast<const ADDR &>(dst);
        return SendMessageInternal(src, dstAddr, m);
    }

    virtual bool
    SendMessageToReplica(TransportReceiver *src,
                         int replicaIdx,
                         const Message &m) override
    {
        ASSERT(this->replicaGroups.find(src) != this->replicaGroups.end());
        int groupIdx = this->replicaGroups[src] == -1 ? 0 : this->replicaGroups[src];
        return SendMessageToReplica(src, groupIdx, replicaIdx, m);
    }

    virtual bool
    SendMessageToReplica(TransportReceiver *src,
                         int groupIdx,
                         int replicaIdx,
                         const Message &m) override
    {
        const specpaxos::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        auto kv = replicaAddresses[cfg][groupIdx].find(replicaIdx);
        ASSERT(kv != replicaAddresses[cfg][groupIdx].end());

        return SendMessageInternal(src, kv->second, m);
    }

    virtual bool SendMessageToFC(TransportReceiver *src, const Message &m) override
    {
        const specpaxos::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        auto kv = fcAddresses.find(cfg);
        if (kv == fcAddresses.end()) {
            Panic("Configuration has no failure coordinator address");
        }

        return SendMessageInternal(src, kv->second, m);
    }

    virtual bool
    SendMessageToAll(TransportReceiver *src,
                     const Message &m)
    {
        ASSERT(this->replicaGroups.find(src) != this->replicaGroups.end());
        int groupIdx = this->replicaGroups[src] == -1 ? 0 : this->replicaGroups[src];
        const specpaxos::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        return SendMessageToGroup(src, groupIdx, m);
    }

    virtual bool
    SendMessageToAllGroups(TransportReceiver *src,
                           const Message &m)
    {
        const specpaxos::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        const ADDR &srcAddr = dynamic_cast<const ADDR &>(src->GetAddress());
        for (auto & kv : replicaAddresses[cfg]) {
            for (auto & kv2 : kv.second) {
                if (srcAddr == kv2.second) {
                    continue;
                }
                if (!SendMessageInternal(src, kv2.second, m)) {
                    return false;
                }
            }
        }
        return true;
    }

    virtual bool
    SendMessageToGroup(TransportReceiver *src,
                       int groupIdx,
                       const Message &m) override
    {
        return SendMessageToGroups(src, std::vector<int>{groupIdx}, m);
    }

    virtual bool
    SendMessageToGroups(TransportReceiver *src,
                        const std::vector<int> &groups,
                        const Message &m) override
    {
        const specpaxos::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        const ADDR &srcAddr = dynamic_cast<const ADDR &>(src->GetAddress());
        for (int groupIdx : groups) {
            for (auto & kv : replicaAddresses[cfg][groupIdx]) {
                if (srcAddr == kv.second) {
                    continue;
                }
                if (!SendMessageInternal(src, kv.second, m)) {
                    return false;
                }
            }
        }
        return true;
    }

    virtual bool
    OrderedMulticast(TransportReceiver *src,
                     const std::vector<int> &groups,
                     const Message &m) override
    {
        Panic("Transport implementation does not support OrderedMulticast");
    }

    virtual bool
    OrderedMulticast(TransportReceiver *src,
                     const Message &m) override
    {
        std::vector<int> groups = {0};
        return OrderedMulticast(src, groups, m);
    }

protected:
    virtual bool SendMessageInternal(TransportReceiver *src,
                                     const ADDR &dst,
                                     const Message &m) = 0;
    virtual ADDR LookupAddress(const specpaxos::Configuration &cfg,
                               int groupIdx,
                               int replicaIdx) = 0;
    virtual const ADDR *
    LookupMulticastAddress(const specpaxos::Configuration *cfg) = 0;
    virtual const ADDR *
    LookupFCAddress(const specpaxos::Configuration *cfg) = 0;

    std::unordered_map<specpaxos::Configuration,
        specpaxos::Configuration *> canonicalConfigs;
    std::map<TransportReceiver *,
        specpaxos::Configuration *> configurations;
    std::map<const specpaxos::Configuration *,
        std::map<int, std::map<int, ADDR> > > replicaAddresses; // config->groupid->replicaid->ADDR
    std::map<const specpaxos::Configuration *,
        std::map<int, std::map<int, TransportReceiver *> > > replicaReceivers;
    std::map<const specpaxos::Configuration *, ADDR> multicastAddresses;
    std::map<const specpaxos::Configuration *, ADDR> fcAddresses;
    std::map<TransportReceiver *, int> replicaGroups;
    bool replicaAddressesInitialized;

    /* configs is a map of groupIdx to Configuration */
    virtual specpaxos::Configuration *
    RegisterConfiguration(TransportReceiver *receiver,
                          const specpaxos::Configuration &config,
                          int groupIdx,
                          int replicaIdx) {
        ASSERT(receiver != NULL);

        // Have we seen this configuration before? If so, get a
        // pointer to the canonical copy; if not, create one. This
        // allows us to use that pointer as a key in various
        // structures.
        specpaxos::Configuration *canonical
            = canonicalConfigs[config];
        if (canonical == NULL) {
            canonical = new specpaxos::Configuration(config);
            canonicalConfigs[config] = canonical;
        }
        // Record configuration
        configurations[receiver] = canonical;

        // If this is a replica, record the receiver
        if (replicaIdx != -1) {
            ASSERT(groupIdx != -1);
            replicaReceivers[canonical][groupIdx][replicaIdx] = receiver;
        }

        // Record which group this receiver belongs to
        replicaGroups[receiver] = groupIdx;

        // Mark replicaAddreses as uninitalized so we'll look up
        // replica addresses again the next time we send a message.
        replicaAddressesInitialized = false;
        return canonical;
    }

    virtual void
    LookupAddresses()
    {
        // Clear any existing list of addresses
        replicaAddresses.clear();
        multicastAddresses.clear();
        fcAddresses.clear();

        // For every configuration, look up all addresses and cache
        // them.
        for (auto &kv : canonicalConfigs) {
            specpaxos::Configuration *cfg = kv.second;

            for (int i = 0; i < cfg->g; i++) {
                for (int j = 0; j < cfg->n; j++) {
                    const ADDR addr = LookupAddress(*cfg, i, j);
                    replicaAddresses[cfg][i].insert(std::make_pair(j, addr));
                }
            }

            // And check if there's a multicast address
            if (cfg->multicast()) {
                const ADDR *addr = LookupMulticastAddress(cfg);
                if (addr) {
                    multicastAddresses.insert(std::make_pair(cfg, *addr));
                    delete addr;
                }
            }
            // Failure coordinator address
            if (cfg->fc()) {
                const ADDR *addr = LookupFCAddress(cfg);
                if (addr) {
                    fcAddresses.insert(std::make_pair(cfg, *addr));
                    delete addr;
                }
            }
        }

        replicaAddressesInitialized = true;
    }
};

#endif // _LIB_TRANSPORTCOMMON_H_
