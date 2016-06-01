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

NODE_TTL = timedelta(minutes=5)


list_cache = None
LIST_TTL = timedelta(seconds=10)


class ListHandler(tornado.web.RequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get(self):
        global list_cache
        if "gzip" not in self.request.headers.get("Accept-Encoding", ""):
            self.set_status(400)
            return

        if list_cache and (datetime.utcnow() - list_cache[1]) < LIST_TTL:
            gzip_json = list_cache[0]
        else:
            out = list(nodes.values())
            out.extend(n for n in irc_client.nodes.values() if n.steamid not in nodes)
            json = ujson.dumps({'nodes': list(x._asdict() for x in out)},
                               ensure_ascii=False)
            gzip_value = BytesIO()
            with gzip.GzipFile(mode="w", fileobj=gzip_value, compresslevel=6) as f:
                f.write(json.encode('utf-8'))
            gzip_json = gzip_value.getvalue()
            list_cache = (gzip_json, datetime.utcnow())
        self.set_header("Content-Encoding", "gzip")
        self.set_header('Content-Type', 'application/json')
        self.write(gzip_json)


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
    ])


@asyncio.coroutine
def expire_nodes():
    while True:
        yield from asyncio.sleep(10)
        now = datetime.utcnow()
        for steamid, last in list(last_seen.items()):
            if now - last > NODE_TTL:
                del nodes[steamid]
                del last_seen[steamid]


@asyncio.coroutine
def publish_in_irc():
    while True:
        yield from asyncio.sleep(110)
        logger.info('Publishing {} nodes to IRC'.format(len(nodes)))
        irc_client.publish_nodes(nodes.values())


def main():
    global irc_client
    logging.basicConfig(level=logging.DEBUG)
    AsyncIOMainLoop().install()
    event_loop = asyncio.get_event_loop()
    app = make_app()
    app.listen(8811)
    irc_client = irc.Client(event_loop)
    asyncio.async(expire_nodes())
    asyncio.async(publish_in_irc())
    event_loop.run_forever()


if __name__ == "__main__":
    main()
