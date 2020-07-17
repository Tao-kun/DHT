import asyncio
import binascii
import glob
import ipaddress
import logging
import os
import random
import signal
import time

from socket import inet_ntoa
from struct import unpack

import aiofiles
import aiomysql
import bencoder
import chardet
from mala import get_metadata

import base_sql

connect_dict = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'ExaMp1e_pAssW0rd',
    'db': 'dht',
    'charset': 'utf8mb4'
}
logging.basicConfig(level=logging.INFO)


def proper_infohash(infohash):
    if isinstance(infohash, bytes):
        # Convert bytes to hex
        infohash = binascii.hexlify(infohash).decode('utf-8')
    return infohash.upper()


def random_node_id(size=20):
    return os.urandom(size)


def split_nodes(nodes):
    length = len(nodes)
    if (length % 26) != 0:
        return

    for i in range(0, length, 26):
        nid = nodes[i:i + 20]
        ip = inet_ntoa(nodes[i + 20:i + 24])
        port = unpack("!H", nodes[i + 24:i + 26])[0]
        yield nid, ip, port


def split_addr(addr_list):
    for addr in addr_list:
        ip = ipaddress.ip_address(addr[:-2])
        port = unpack('>H', addr[-2:])
        yield ip, port


def get_filename(meta_info):
    if b'name.utf-8' in meta_info.keys():
        return meta_info[b'name.utf-8'].decode()
    else:
        file_name = meta_info[b'name']
        try:
            return file_name.decode()
        except UnicodeDecodeError:
            encoding = chardet.detect(file_name)
            return file_name.decode(encoding['encoding'])


def get_file_size(meta_info):
    if b'length' in meta_info.keys():
        length = meta_info[b'length']
    else:
        length = 0
        for file in meta_info[b'files']:
            length += file[b'length']
    return length


BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ('tracker.openbittorrent.com', 80),
    ('tracker.opentrackr.org', 1337)
)


class Node(object):
    def __init__(self, node_id, addr, last_ping=None):
        self.node_id = node_id
        self.addr = addr
        if last_ping is None:
            self.last_ping = time.time()
        else:
            self.last_ping = last_ping


class RouteTable(object):
    def __init__(self, size=256):
        self.nodes = dict()
        self.size = size

    def __remove_unactived(self):
        keys = list(self.keys())
        keys.sort(key=lambda x: self[x].last_ping)
        self.pop(keys[0])

    def __contains__(self, item):
        return item in self.nodes.keys()

    def __len__(self):
        return len(self.nodes)

    def __getitem__(self, key):
        return self.nodes[key]

    def __setitem__(self, key, value):
        if key not in self.nodes.keys():
            self.nodes[key] = Node(key, value)
        else:
            self.nodes[key].addr = value
            self.nodes[key].last_ping = time.time()

        if len(self.nodes) > self.size:
            self.__remove_unactived()

    def __iter__(self):
        return iter(self.nodes.values())

    def keys(self):
        return self.nodes.keys()

    def pop(self, node_id):
        if node_id in self.nodes.keys():
            return self.nodes.pop(node_id)
        return None


class Crawler(asyncio.DatagramProtocol):
    """
    This class' implementation is from https://github.com/whtsky/maga/blob/master/maga.py
    """

    def __init__(self, loop=None, bootstrap_nodes=BOOTSTRAP_NODES, interval=3):
        self.node_id = random_node_id()
        self.table = RouteTable()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        self.connection_pool = self.loop.run_until_complete(aiomysql.create_pool(loop=self.loop, **connect_dict))
        self.database_semaphore = asyncio.Semaphore(64)
        self.fetch_metainfo_semaphore = asyncio.Semaphore(128)
        self.bootstrap_nodes = bootstrap_nodes
        self.__running = False
        self.interval = interval

    def stop(self):
        self.__running = False
        self.loop.call_later(self.interval, self.loop.stop)

    async def auto_find_nodes(self):
        self.__running = True
        while self.__running:
            await asyncio.sleep(self.interval)
            for node in self.bootstrap_nodes:
                self.find_node(addr=node)

    def run(self, port=6881):
        coroutine = self.loop.create_datagram_endpoint(
            lambda: self, local_addr=('0.0.0.0', port)
        )
        transport, _ = self.loop.run_until_complete(coroutine)

        for signal_name in ('SIGINT', 'SIGTERM'):
            try:
                self.loop.add_signal_handler(getattr(signal, signal_name), self.stop)
            except NotImplementedError:
                # SIGINT and SIGTERM are not implemented on windows
                pass

        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(addr=node)

        asyncio.ensure_future(self.info_looger(), loop=self.loop)
        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        asyncio.ensure_future(self.auto_get_metainfo(), loop=self.loop)
        self.loop.run_forever()
        self.loop.close()

    def random_node_in_table(self):
        if len(self.table) != 0:
            return self.table[random.choice(list(self.table.keys()))], None
        return None, random_node_id()

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.__running = False
        self.transport.close()

    def send_message(self, data, addr):
        data.setdefault("t", b"tt")
        self.transport.sendto(bencoder.bencode(data), addr)

    def find_node(self, addr, target_node_id=None):
        if not target_node_id:
            target, _ = self.random_node_in_table()
            if target is None:
                target_node_id = _
            else:
                if target.addr == addr:
                    return
                target_node_id = target.node_id
        self.send_message({
            "t": "aa",
            "y": "q",
            "q": "find_node",
            "a": {
                "id": self.node_id,
                "target": target_node_id
            }
        }, addr=addr)

    def datagram_received(self, data, addr):
        try:
            msg = bencoder.bdecode(data)
        except:
            return
        try:
            self.handle_message(msg, addr)
        except Exception as e:
            self.send_message(data={
                "t": msg["t"],
                "y": "e",
                "e": [202, "Server Error"]
            }, addr=addr)
            raise e

    def handle_message(self, msg, addr):
        msg_type = msg.get(b"y", b"e")
        if msg_type == b"e":
            return
        if msg_type == b"r":
            return self.handle_response(msg, addr=addr)
        if msg_type == b'q':
            return asyncio.ensure_future(
                self.handle_query(msg, addr=addr), loop=self.loop
            )

    def handle_response(self, msg, addr):
        args = msg[b"r"]
        node_id = args[b"id"]
        self.table[node_id] = addr
        if b"nodes" in args:
            for node_id, ip, port in split_nodes(args[b"nodes"]):
                self.ping(addr=(ip, port))

    async def handle_query(self, msg, addr):
        args = msg[b"a"]
        node_id = args[b"id"]
        query_type = msg[b"q"]
        if node_id == self.node_id:
            return
        self.table[node_id] = addr
        if query_type == b"get_peers":
            infohash = args[b"info_hash"]
            infohash = proper_infohash(infohash)
            token = infohash[:2]
            self.send_message({
                "t": msg[b"t"],
                "y": "r",
                "r": {
                    "id": self.node_id,
                    "nodes": "",
                    "token": token
                }
            }, addr=addr)
            await self.handle_get_peers(infohash, addr)
        elif query_type == b"announce_peer":
            infohash = args[b"info_hash"]
            target_id = msg[b"t"]
            self.send_message({
                "t": target_id,
                "y": "r",
                "r": {
                    "id": self.node_id
                }
            }, addr=addr)
            peer_addr = [addr[0], addr[1]]
            try:
                peer_addr[1] = args[b"port"]
            except KeyError:
                pass
            await self.handle_announce_peer(proper_infohash(infohash), addr, peer_addr)
        elif query_type == b"find_node":
            target_id = msg[b"t"]
            self.send_message({
                "t": target_id,
                "y": "r",
                "r": {
                    "id": self.node_id,
                    "nodes": ""
                }
            }, addr=addr)
        elif query_type == b"ping":
            self.send_message({
                "t": "tt",
                "y": "r",
                "r": {
                    "id": self.node_id
                }
            }, addr=addr)
        self.find_node(addr=addr)

    def ping(self, addr):
        self.send_message({
            "y": "q",
            "t": "pg",
            "q": "ping",
            "a": {
                "id": self.node_id
            }
        }, addr=addr)

    async def handle_get_peers(self, infohash, addr):
        # logging.info(
        #    "Receive get peers message from DHT {}. Infohash: {}.".format(
        #        addr, infohash
        #    )
        # )
        # if len(infohash) != 40:
        #    return
        pass

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        # logging.info(
        #    "Receive announce peer message from DHT {}. Infohash: {}. Peer address:{}".format(
        #        addr, infohash, peer_addr
        #    )
        # )
        if len(infohash) != 40:
            return
        async with self.database_semaphore:
            async with self.connection_pool.acquire() as connect:
                cursor = await connect.cursor()
                await cursor.execute(base_sql.insert_into_announce_queue.format(
                    info_hash=infohash,
                    ip_addr=peer_addr[0],
                    port=peer_addr[1]))
                await connect.commit()
                await cursor.close()

    async def get_metainfo(self, infohash, addr):
        async with self.fetch_metainfo_semaphore:
            filename = '/root/torrent/{}.torrent'.format(infohash.lower())
            if len(glob.glob(filename)) == 0:
                metainfo = await get_metadata(
                    infohash, addr[0], addr[1], loop=self.loop
                )
                if isinstance(metainfo, bool) and metainfo is False:
                    async with self.database_semaphore:
                        async with self.connection_pool.acquire() as connect:
                            cursor = await connect.cursor()
                            await cursor.execute(base_sql.remove_from_announce_queue.format(info_hash=infohash))
                            await connect.commit()
                            await cursor.close()
                            return
                if metainfo is not None:
                    name = get_filename(metainfo)
                    size = get_file_size(metainfo)
                    logging.info(size, name, infohash)
                    file_content = bencoder.bencode({b'info': metainfo})
                    async with aiofiles.open(filename, mode='wb') as f:
                        await f.write(file_content)
                    async with self.database_semaphore:
                        async with self.connection_pool.acquire() as connect:
                            cursor = await connect.cursor()
                            await cursor.execute(base_sql.insert_into_torrent.format(
                                name=aiomysql.escape_string(name),
                                info_hash=infohash,
                                size=size))
                            await cursor.execute(base_sql.remove_from_announce_queue.format(info_hash=infohash))
                            await connect.commit()
                            await cursor.close()

    async def auto_get_metainfo(self):
        async with self.database_semaphore:
            async with self.connection_pool.acquire() as connect:
                cursor = await connect.cursor()
                await cursor.execute(base_sql.clean_announce_queue)
                await connect.commit()
                await cursor.close()
        while self.__running:
            async with self.database_semaphore:
                async with self.connection_pool.acquire() as connect:
                    cursor = await connect.cursor()
                    await cursor.execute(base_sql.get_size_in_announce_queue)
                    (data,) = await cursor.fetchone()
                    if data == 0:
                        await asyncio.sleep(self.interval)
                        continue
                    await cursor.execute(base_sql.get_one_in_announce_queue)
                    data = await cursor.fetchone()
                    infohash = data[0]
                    peer_addr = (data[1], data[2])
                    await cursor.execute(base_sql.set_lock.format(info_hash=infohash))
                    await connect.commit()
                    await cursor.close()
            asyncio.ensure_future(self.get_metainfo(infohash, peer_addr), loop=self.loop)

    async def handler(self, infohash, addr):
        pass

    async def info_looger(self):
        while self.__running:
            async with self.database_semaphore:
                async with self.connection_pool.acquire() as connect:
                    cursor = await connect.cursor()
                    await cursor.execute(base_sql.torrent_count)
                    (torrent_count,) = await cursor.fetchone()
                    await cursor.execute(base_sql.announce_queue_count)
                    (announce_queue_count,) = await cursor.fetchone()
                    await connect.commit()
                    await cursor.close()
            logging.info(
                "There are {} torrents' information in database. Fetch {} torrent(s) now.".format(
                    torrent_count, announce_queue_count)
            )
            await asyncio.sleep(self.interval * 10)


if __name__ == '__main__':
    crawl = Crawler()
    crawl.run()
