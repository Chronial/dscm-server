import asyncio
import json
import logging
from datetime import datetime, timedelta

import tornado.ioloop
import tornado.web
from tornado.platform.asyncio import AsyncIOMainLoop
from darksouls import DSCMNode

import irc

logger = logging.getLogger(__name__)

irc_client = None

nodes = dict()
last_seen = dict()

NODE_TTL = timedelta(minutes=5)


class ListHandler(tornado.web.RequestHandler):
    def get(self):
        out = list(nodes.values())
        out.extend(n for n in irc_client.nodes.values() if n.steamid not in nodes)
        self.write({'nodes': out})


class StoreHandler(tornado.web.RequestHandler):
    def post(self):
        now = datetime.utcnow()
        data = json.loads(self.request.body.decode('utf-8'))
        self_node = DSCMNode(**data['self'])
        nodes[self_node.steamid] = self_node
        last_seen[self_node.steamid] = now
        for node_dict in data['nodes']:
            node = DSNode(**node_dict)
            if isinstance(nodes.get(node.steamid), DSCMNode):
                node = nodes[node.steamid]._replace(node._asdict())
            nodes[node.steamid] = node
            last_seen[node.steamid] = now


def make_app():
    settings = {
        'compress_response': True,
    }
    return tornado.web.Application([
        (r"/list", ListHandler),
        (r"/store", StoreHandler),
    ], **settings)


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
