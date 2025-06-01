- [ ] packet loss handling:
  - [ ] connect: need to retry SYN packets
  - [x] MTU probing: send more than one probe to reduce corruption due to packet loss
  - [ ] MTU probing: don't probe if packet loss is likely. Probe only when the network conditions are stable.
        This can be measured with e.g. RTT variance.

- [x] rtt: must measure before updating congestion controller
  - [x] rtt isn't set at all in certain cases, which causes then cwnd computation to be incorrect
- [x] RTO: only send one packet

- [x] MTU probing: don't probe consecutive segments, otherwise it'll timeout several times in a row
- [ ] MTU probing: probes should happen on a timer basis, not on number of packets
- [ ] MTU probing: set minimum default MSS to smth less conservative than 576

With dontfrag:
- [x] error sending to UDP socket addr=45.142.177.18:5001, len=1356: Message too long (os error 40)
- [x] retransmit: looks like we are resetting the timer too often. We must not reset it if no new data is acked.
      newly set packets should arm, but NOT rearm the timer


Real world:
- [x] Spammy message, seems to occur in a loop mostly: RTO expired, rewinding to retransmit FIN

Code:
- [ ] maybe_send_syn_ack() - timer handling is a bit custom, need to use RTO and timers framework
