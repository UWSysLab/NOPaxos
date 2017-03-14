// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * sequencer/sequencer.h:
 *   End-host network sequencer implementation.
 *
 * Copyright 2017 Jialin Li <lijl@cs.washington.edu>
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

#ifndef __SEQUENCER_H__
#define __SEQUENCER_H__

#include <arpa/inet.h>
#include <linux/if_packet.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <net/if.h>
#include <netinet/ether.h>

#include <unordered_map>

namespace sequencer {

class Sequencer {
public:
    Sequencer(uint64_t sequencer_id);
    ~Sequencer();

    uint64_t Increment(uint32_t groupIdx);
    uint64_t GetSequencerID() { return this->sequencer_id; };

private:
    std::unordered_map<uint32_t, uint64_t> counters;
    uint64_t sequencer_id;
};

class Configuration {
public:
    Configuration(std::ifstream &file);
    ~Configuration();

    std::string GetInterface();
    std::string GetGroupAddr();

private:
    std::string interface;
    std::string groupAddr;
};

class Transport {
public:
    Transport(Sequencer *sequencer, Configuration *config);
    ~Transport();
    void Run();

private:
    static const int ETHER_TYPE = 0x0800;
    static const int BUFFER_SIZE = 16384;
    static const int NONFRAG_MAGIC = 0x20050318;
    Sequencer *sequencer;
    Configuration *config;
    int sockfd;
    struct sockaddr_ll destSockAddr;

    bool ProcessPacket(uint8_t *packet, size_t len);
};

} // namespace sequencer

#endif /* __SEQUENCER_H__ */
