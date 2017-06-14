import asyncio
import logging
import gzip
from datetime import datetime, timedelta
from io import BytesIO

import tornado.httpserver
import tornado.ioloop
import tornado.web
import ujson
from tornado.platform.asyncio import AsyncIOMainLoop
from darksouls import DSCMNode, DSNode

logger = logging.getLogger(__name__)

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
            json = ujson.dumps({'nodes': [x._asdict() for x in nodes.values()]},
                               ensure_ascii=False)
            gzip_value = BytesIO()
            with gzip.GzipFile(mode="w", fileobj=gzip_value, compresslevel=6) as f:
                f.write(json.encode('utf-8'))
            gzip_json = gzip_value.getvalue()
            list_cache = (gzip_json, datetime.utcnow())
        self.set_header("Content-Encoding", "gzip")
        self.set_header('Content-Type', 'application/json')
        self.set_header('Connection', 'close')
        self.write(gzip_json)


class StatusHandler(tornado.web.RequestHandler):
    def get(self):
        out = {
            'total': len(nodes),
            'DSCM': sum(1 for n in nodes.values() if isinstance(n, DSCMNode)),
            'DS': sum(1 for n in nodes.values() if isinstance(n, DSNode)),
        }
        self.set_header('Content-Type', 'application/json')
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
        self.set_header('Connection', 'close')


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


def main():
    logging.basicConfig(level=logging.DEBUG)
    AsyncIOMainLoop().install()
    event_loop = asyncio.get_event_loop()
    app = make_app()
    http_server = tornado.httpserver.HTTPServer(app, no_keep_alive=True)
    http_server.listen(8811)
    asyncio.async(expire_nodes())
    event_loop.run_forever()


if __name__ == "__main__":
    main()
