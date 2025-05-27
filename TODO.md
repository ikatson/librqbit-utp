- [x] rtt: must measure before updating congestion controller
  - [x] rtt isn't set at all in certain cases, which causes then cwnd computation to be incorrect
- [x] RTO: only send one packet

- [ ] MTU probing: don't probe consecutive segments, otherwise it'll timeout several times in a row
