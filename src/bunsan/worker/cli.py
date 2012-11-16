import signal

import bunsan.worker
from bunsan.worker.dcs import Hub

import bunsan.pm


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser("bunsan::worker")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.0.1', help="version information")
    parser.add_argument('-l', '--listen', action='store', dest='addr', help='Listen on addr:port', required=True)
    parser.add_argument('-d', '--hub', action='store', dest='hub_uri', help='hub xmlrpc interface', required=True)
    parser.add_argument('-m', '--machine', action='store', dest='machine', help='machine name', required=True)
    parser.add_argument('-c', '--worker-count', action='store', dest='worker_count', type=int, help='worker count', default=1)
    parser.add_argument('-r', '--repository-config', action='store', dest='repository_config', help='path to repository config', required=True)
    parser.add_argument('-s', '--resource', action='append', dest='resources', help='resources in format resource_id=resource_uri')
    parser.add_argument('-t', '--tmpdir', action='store', dest='tmpdir', help='temporary directory path', default='/tmp')
    parser.add_argument('-T', '--timeout', action='store', dest='timeout', type=int, help='connection timeout', default='10')
    args = parser.parse_args()
    host, port = tuple(args.addr.split(':'))
    port = int(port)
    def split_resource(s):
        pos = s.index('=')
        return (s[:pos], s[pos + 1:])
    resources = list(map(split_resource, args.resources or []))
    worker = bunsan.worker.Worker(
        addr=(host, port),
        worker_count=args.worker_count,
        resources=resources,
        repository=bunsan.pm.Repository(args.repository_config),
        hub=Hub(hub_uri=args.hub_uri, machine=args.machine, timeout=args.timeout),
        tmpdir=args.tmpdir)
    worker.serve_forever(signals=[signal.SIGTERM])
