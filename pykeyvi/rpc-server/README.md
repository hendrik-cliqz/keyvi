## Experimental kevyi store implementation

To run it, start the server with:

    python mprpcapp.py -c rpc_server_conf.py 'rpc_server:KeyviServer'

Start the merger daemon with:

    python merger.py

Use mprpc to communicate with the server:

    from mprpc import RPCClient
    c=RPCClient('localhost', 9100)
    c.call('set', 'dilbert', "{'what':'developer'}")
    c.call('set', 'wally', "{'what':'developer'}")
    c.call('set', 'phb', "{'what':'manager'}")
    c.call('commit')

    x=c.call('get', 'dilbert')
    import pykeyvi
    pykeyvi.Match.loads(x).GetValue()

### How it works

Data is collected until you call 'commit'. Every 'commit' creates a segment, watch the kv-index subfolder. 
The merger daemon picks up segments and merges them.
The index.toc file is used to keep a list of segments that define the current index. That file is checked/read from the worker processes.

Watch the kv-index subfolder to get an idea.

### Beware

This is hacked in very short time, full of bugs, not scalable, probably containing various concurrency issues.
