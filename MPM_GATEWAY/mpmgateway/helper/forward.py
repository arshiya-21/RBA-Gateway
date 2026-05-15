import getpass
import os
import socket
import select
import threading
from optparse import OptionParser
import paramiko
import socketserver
import sys
SSH_PORT = 22
DEFAULT_PORT = 4000

g_verbose = True

class ForwardServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            chan = self.ssh_transport.open_channel(
                "direct-tcpip",
                (self.chain_host, self.chain_port),
                self.request.getpeername(),
            )
        except Exception as e:
            verbose(
                "Incoming request to %s:%d failed: %s"
                % (self.chain_host, self.chain_port, repr(e))
            )
            return
        if chan is None:
            verbose(
                "Incoming request to %s:%d was rejected by the SSH server."
                % (self.chain_host, self.chain_port)
            )
            return

        verbose(
            "Connected!  Tunnel open %r -> %r -> %r"
            % (
                self.request.getpeername(),
                chan.getpeername(),
                (self.chain_host, self.chain_port),
            )
        )
        try:
            while not self.stop_event1.is_set():
                r, w, x = select.select([self.request, chan], [], [], 1)
                if self.request in r:
                    data = self.request.recv(1024)
                    if len(data) == 0:
                        break
                    chan.send(data)
                if chan in r:
                    data = chan.recv(1024)
                    if len(data) == 0:
                        break
                    self.request.send(data)
        except OSError as e:
            print(f"Error during tunnel operation: {e}")
        finally:
            print("tunnel is closing....")
            chan.close()
            self.request.close()
            print("Tunnel closed")

def forward_tunnel(local_port, remote_host, remote_port, transport, stop_event):
    class SubHandler(Handler):
        chain_host = remote_host
        chain_port = remote_port
        ssh_transport = transport
        stop_event1 = stop_event
    server = ForwardServer(("", local_port), SubHandler)
    
    # Run the server in a thread to allow for graceful stopping
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    stop_event.wait()
    server.shutdown()
    server.server_close()
    server_thread.join()

def verbose(s):
    if g_verbose:
        print(s)

HELP = """\
Set up a forward tunnel across an SSH server, using paramiko. A local port
(given with -p) is forwarded across an SSH session to an address:port from
the SSH server. This is similar to the openssh -L option.
"""

def get_host_port(spec, default_port):
    "parse 'hostname:22' into a host and port, with the port optional"
    args = (spec.split(":", 1) + [default_port])[:2]
    args[1] = int(args[1])
    return args[0], args[1]

def parse_options():
    global g_verbose

    parser = OptionParser(
        usage="usage: %prog [options] <ssh-server>[:<server-port>]",
        version="%prog 1.0",
        description=HELP,
    )
    parser.add_option(
        "-q",
        "--quiet",
        action="store_false",
        dest="verbose",
        default=True,
        help="squelch all informational output",
    )
    parser.add_option(
        "-p",
        "--local-port",
        action="store",
        type="int",
        dest="port",
        default=DEFAULT_PORT,
        help="local port to forward (default: %d)" % DEFAULT_PORT,
    )
    parser.add_option(
        "-u",
        "--user",
        action="store",
        type="string",
        dest="user",
        default=getpass.getuser(),
        help="username for SSH authentication (default: %s)"
        % getpass.getuser(),
    )
    parser.add_option(
        "-K",
        "--key",
        action="store",
        type="string",
        dest="keyfile",
        default=None,
        help="private key file to use for SSH authentication",
    )
    parser.add_option(
        "",
        "--no-key",
        action="store_false",
        dest="look_for_keys",
        default=True,
        help="don't look for or use a private key file",
    )
    parser.add_option(
        "-P",
        "--password",
        action="store_true",
        dest="readpass",
        default=False,
        help="read password (for key or password auth) from stdin",
    )
    parser.add_option(
        "-r",
        "--remote",
        action="store",
        type="string",
        dest="remote",
        default=None,
        metavar="host:port",
        help="remote host and port to forward to",
    )
    options, args = parser.parse_args()

    if len(args) != 1:
        parser.error("Incorrect number of arguments.")
    if options.remote is None:
        parser.error("Remote address required (-r).")

    g_verbose = options.verbose
    server_host, server_port = get_host_port(args[0], SSH_PORT)
    remote_host, remote_port = get_host_port(options.remote, SSH_PORT)
    return options, (server_host, server_port), (remote_host, remote_port)

# Example of usage
if __name__ == "__main__":
    options, server_addr, remote_addr = parse_options()
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())

    print(f"Connecting to ssh host {server_addr[0]}:{server_addr[1]}...")
    try:
        client.connect(
            server_addr[0],
            server_addr[1],
            username=options.user,
            key_filename=options.keyfile,
            look_for_keys=options.look_for_keys,
            password=getpass.getpass() if options.readpass else None,
        )
    except Exception as e:
        print(f"Failed to connect to {server_addr[0]}:{server_addr[1]}: {str(e)}")
        sys.exit(1)

    print(
        f"Now forwarding port {options.port} to {remote_addr[0]}:{remote_addr[1]} ..."
    )

    stop_event = threading.Event()

    try:
        forward_tunnel(
            options.port, remote_addr[0], remote_addr[1], client.get_transport(), stop_event
        )
    except KeyboardInterrupt:
        print("Port forwarding stopped.")
        stop_event.set()
    finally:
        client.close()
        print("SSH client connection closed.")
