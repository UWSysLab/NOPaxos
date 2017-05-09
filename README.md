# Network-Ordered Paxos

This is an implementation of the Network-Ordered Paxos (NOPaxos) protocol, as
described in the paper
["Just Say NO to Paxos Overhead: Replacing Consensus with Network Ordering"](http://homes.cs.washington.edu/~lijl/papers/nopaxos-osdi16.pdf)
from OSDI 2016.

NOPaxos is a state machine replication protocol based on the
idea of co-designing distributed systems with the datacenter
network. NOPaxos divides replication responsibility between the
network and protocol layers. The network orders requests but does
not ensure reliable delivery -- using a new network primitive,
Ordered Unreliable Multicast (OUM). The replication protocol exploits
network ordering to provide strongly consistent replication without
coordination.

In normal case, NOPaxos avoids coordination entirely by relying on the
network to deliver messages in the same order. It requires application-level
coordination only to handle dropped packets. The resulting protocol is simple,
achieving near-optimal throughput and latency, and remains robust to
network-level failures. It not only outperforms both latency- and throughput-optimized protocols on their respective metrics, but also yields throughput within 2% and latency within 16 us of an unreplicated system -- providing replication without the performance cost.

## Contents

This repository contains implementations of 5 replication protocols:

1. Network Ordered Paxos, including normal operation, gap agreement,
   view change, and synchronization protocols.

2. Speculative Paxos, including normal operation, synchronization, and
reconciliation protocols.

3. Viewstamped Replication (VR), aka Multi-Paxos, as described in the
   paper
   ["Viewstamped Replication Revisited"](http://pmg.csail.mit.edu/papers/vr-revisited.pdf),
   including an optional batching optimization

4. Fast Paxos, although only the normal case is implemented.

5. A simple unreplicated RPC protocol for comparison

...as well as an endhost implementation of the sequencer.

## Building and Running

NOPaxos and can be built using `make`. It has been tested on Ubuntu 14.04,
16.04 and Debian 8. Regression tests can be run with `make check`

Dependencies include (Debian/Ubuntu packages):
  protobuf-compiler pkg-config libunwind-dev libssl-dev libprotobuf-dev libevent-dev libgtest-dev

You will need to create a configuration file with the following
syntax:

```
f <number of failures tolerated>
replica <hostname>:<port>
replica <hostname>:<port>
...
multicast <multicast addr>:<port>
```

Multicast address is optional. However, the Ordered Unreliable Multicast (OUM) implementation
uses the multicast address as the OUM group address. For testing, a reasonable option is to use the broadcast
address as multicast address.

In order to run NOPaxos, you need to configure the network to route OUM packets first to the
sequencer, and then multicast to all OUM receivers. How this is implemented would depend on 
network design. The easiest way is to use OpenFlow and install rules that match on the multicast address.

Our OUM implementation expects a custom header at the beginning of the UDP data, which contains
the session number and message number described in the paper. If you need to write your own
sequencer, please refer to `sequencer/sequencer.cc` for the custom header format. If you need to
modify the header format, you will have to modify `ProcessPacket` and `DecodePacket` in
`lib/udptransport.cc`.

The endhost sequencer also requires a configuration file with the following syntax:

```
interface <network interface name>
groupaddr <multicast addr>
```

The endhost sequencer uses raw sockets, so you will need to provide the network interface name. `groupaddr`
is the same as the multicast address used in the previous configuration. You can start the endhost
sequencer with `sudo ./sequencer/sequencer -c <path to sequencer config file>` (sudo is required
because of the use of raw sockets)

You can then start a replica with `./bench/replica -c <path to config file> -i <replica number> -m <mode>`, where `mode` is either:
  - `nopaxos` for Network Ordered Paxos
  - `spec` for Speculative Paxos
  - `vr` for Viewstamped Replication
  - `fastpaxos` for Fast Paxos
  - `unreplicated` for no replication (uses only the first replica)

The `vr` mode also accepts a `-b` option to specify the maximum batch
size.

To run a single client, use `./bench/client -c <path to config file>
-m <mode>`.

For performance measurements, you will likely want to add `-DNASSERT`
and `-O2` to the `CFLAGS` in the Makefile, and run `make PARANOID=0`,
which disables complexity-changing assertions.

## Contact

NOPaxos is a product of the
[UW Systems Lab](http://syslab.cs.washington.edu/). Please email Jialin
Li at lijl@cs.washington.edu with any questions.
