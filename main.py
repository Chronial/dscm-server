import asyncio
import logging
import gzip
from datetime import datetime, timedelta
from io import BytesIO

import tornado.ioloop
import tornado.web
import ujson
from tornado.platform.asyncio import AsyncIOMainLoop
from darksouls import DSCMNode, DSNode

import irc

logger = logging.getLogger(__name__)

irc_client = None

nodes = dict()
last_seen = dict()

DSCM_NODE_TTL = timedelta(minutes=2)
# Remember non-DSCM nodes for longer as they might not have quit but just lost
# all connections to DSCM nodes
DS_NODE_TTL = timedelta(minutes=10)


list_cache = None
LIST_TTL = timedelta(seconds=10)


class ListHandler(tornado.web.RequestHandler):
    def get(self):
        global list_cache
        if "gzip" not in self.request.headers.get("Accept-Encoding", ""):
            self.set_status(400)
            return

        if list_cache and (datetime.utcnow() - list_cache[1]) < LIST_TTL:
            gzip_json = list_cache[0]
        else:
            out = dict(nodes)
            for node in irc_client.nodes.values():
                if isinstance(node, DSCMNode) or node.steamid not in nodes:
                    out[node.steamid] = node
            json = ujson.dumps({'nodes': list(x._asdict() for x in out.values())},
                               ensure_ascii=False)
            gzip_value = BytesIO()
            with gzip.GzipFile(mode="w", fileobj=gzip_value, compresslevel=6) as f:
                f.write(json.encode('utf-8'))
            gzip_json = gzip_value.getvalue()
            list_cache = (gzip_json, datetime.utcnow())
        self.set_header("Content-Encoding", "gzip")
        self.set_header('Content-Type', 'application/json')
        self.write(gzip_json)


class StatusHandler(tornado.web.RequestHandler):
    def get(self):
        data = {
            'http': nodes,
            'irc': irc_client.nodes,
        }
        out = dict()
        for name, nodedict in data.items():
            out[name] = {
                'total': len(nodedict),
                'DSCM': sum(1 for n in nodedict.values() if isinstance(n, DSCMNode)),
                'DS': sum(1 for n in nodedict.values() if isinstance(n, DSNode)),
            }
        out['total'] = dict()
        out['total']['total'] = len(set(nodes.keys()) | set(irc_client.nodes.keys()))
        out['total']['DSCM'] = out['http']['DSCM'] + out['irc']['DSCM']
        out['total']['DS'] = out['total']['total'] - out['total']['DSCM']
        self.set_header('Content-Type', 'text/plain')
        self.write(ujson.dumps(out, indent=2))


class StoreHandler(tornado.web.RequestHandler):
    def post(self):
        now = datetime.utcnow()
        data = ujson.loads(self.request.body.decode('utf-8'))
        self_node = DSCMNode(**data['self'])
        nodes[self_node.steamid] = self_node
        last_seen[self_node.steamid] = now
        for node_dict in data['nodes']:
            node = DSNode(**node_dict)
            if isinstance(nodes.get(node.steamid), DSCMNode):
                node = nodes[node.steamid]._replace(**node._asdict())
            nodes[node.steamid] = node
            last_seen[node.steamid] = now


def make_app():
    return tornado.web.Application([
        (r"/list", ListHandler),
        (r"/store", StoreHandler),
        (r"/status", StatusHandler),
    ])


@asyncio.coroutine
def expire_nodes():
    while True:
        yield from asyncio.sleep(10)
        now = datetime.utcnow()
        for steamid, last in list(last_seen.items()):
            ttl = DSCM_NODE_TTL if isinstance(nodes[steamid], DSCMNode) else DS_NODE_TTL
            if now - last > ttl:
                del nodes[steamid]
                del last_seen[steamid]


@asyncio.coroutine
def publish_in_irc():
    while True:
        yield from asyncio.sleep(60*3 + 10)
        logger.info('Publishing {} nodes to IRC'.format(len(nodes)))
        irc_client.publish_nodes(nodes.values())


def main():
    global irc_client
    logging.basicConfig(level=logging.DEBUG)
    AsyncIOMainLoop().install()
    event_loop = asyncio.get_event_loop()
    app = make_app()
    app.listen(8811, no_keep_alive=True)
    irc_client = irc.Client(event_loop)
    asyncio.async(expire_nodes())
    asyncio.async(publish_in_irc())
    event_loop.run_forever()


if __name__ == "__main__":
    main()
