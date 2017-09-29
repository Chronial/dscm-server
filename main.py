import asyncio
import logging
import gzip
from collections import Counter
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
online_ids = dict()

DSCM_NODE_TTL = timedelta(minutes=2)
# Remember non-DSCM nodes for longer as they might not have quit but just lost
# all connections to DSCM nodes
DS_NODE_TTL = timedelta(minutes=10)

ONLINE_IDS_TTL = timedelta(minutes=5)

watch_queue = []
watch_queue_index = 0
watches_handed_out = []


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
        extra_online = (set(online_ids.keys()) -
                        set(int(x, 16) for x in nodes.keys()))

        versions = Counter(x.dscm_version for x in nodes.values()
                           if isinstance(x, DSCMNode))

        out = {
            'versions': dict(versions),
            'total_nodes': len(nodes),
            'DSCM': sum(1 for n in nodes.values() if isinstance(n, DSCMNode)),
            'DS': sum(1 for n in nodes.values() if isinstance(n, DSNode)),
            'extra_online': len(extra_online),
            'online_ids': len(online_ids),
            'total_known': len(extra_online) + len(nodes),
        }
        self.set_header('Content-Type', 'application/json')
        self.write(ujson.dumps(out, indent=2))


class StoreHandler(tornado.web.RequestHandler):
    def post(self):
        now = datetime.utcnow()
        data = ujson.loads(self.request.body.decode('utf-8'))
        user_agent = self.request.headers.get('User-Agent', 'old')
        if user_agent == 'old' and 'online_ids' in data:
            user_agent = 'DSCM/2017.09.28.17'
        self_node = DSCMNode(**data['self'], dscm_version=user_agent)
        nodes[self_node.steamid] = self_node
        last_seen[self_node.steamid] = now
        for node_dict in data['nodes']:
            node = DSNode(**node_dict)
            if isinstance(nodes.get(node.steamid), DSCMNode):
                node = nodes[node.steamid]._replace(**node._asdict())
            nodes[node.steamid] = node
            last_seen[node.steamid] = now

        online_ids.update((x, now) for x in data.get('online_ids', []))
        self.set_header('Connection', 'close')


class WatchHandler(tornado.web.RequestHandler):
    def get(self):
        global watch_queue, watch_queue_index
        to_watch = watch_queue[watch_queue_index]
        watch_queue_index = (watch_queue_index + 1) % len(watch_queue)
        watches_handed_out.append(to_watch)

        out = {
            'watch': "{:016x}".format(to_watch)
        }
        self.set_header('Content-Type', 'application/json')
        self.write(ujson.dumps(out, indent=2))


def make_app():
    return tornado.web.Application([
        (r"/list", ListHandler),
        (r"/store", StoreHandler),
        (r"/status", StatusHandler),
        (r"/get_watch", WatchHandler),
    ])


@asyncio.coroutine
def generate_watch_queue():
    global watch_queue, watch_queue_index
    yield from asyncio.sleep(5)
    while True:
        yield from asyncio.sleep(10)
        extra_online = (set(online_ids.keys()) -
                        set(int(x, 16) for x in nodes.keys()))

        del watches_handed_out[:-2*len(extra_online)]
        handed_out_dict = {v: i for i, v in enumerate(watches_handed_out)}

        # prefer ids that:
        # * have never been handed out or have been handed out earlier (=longer ago)
        # * have been seen more recently
        watch_weights = [
            (handed_out_dict.get(k, -1), -online_ids[k].timestamp(), k) for k in extra_online]
        watch_weights.sort()

        watch_queue = [x[2] for x in watch_weights]
        watch_queue_index = 0


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

        for steamid, last in list(online_ids.items()):
            if now - last > ONLINE_IDS_TTL:
                del online_ids[steamid]


def main():
    logging.basicConfig(level=logging.DEBUG)
    AsyncIOMainLoop().install()
    event_loop = asyncio.get_event_loop()
    app = make_app()
    http_server = tornado.httpserver.HTTPServer(app, no_keep_alive=True)
    http_server.listen(8811)
    asyncio.async(expire_nodes())
    asyncio.async(generate_watch_queue())
    event_loop.run_forever()


if __name__ == "__main__":
    main()
