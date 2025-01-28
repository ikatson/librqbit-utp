## death and cleanup sequence cases

### Case 1. The UtpStream clients died.

When both UtpStream ends are dead, this MUST wake up the VirtualSocket dispatcher.
It should recognize it, and exit.

TODO however, we must give it a little bit of time to send FINs etc.

After it exists, it send a message to UtpSocket to shut it down.

### VirtualSocket dispatcher crashed.
This will send the Shutdown message to UtpSocket immediately.

### Case 2. UtpSocket is dead.

This will crash the dispatcher.
