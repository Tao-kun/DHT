import asyncio
import binascii
import glob
import hashlib
import ipaddress
import logging
import math
import os
import signal
import time

from socket import inet_ntoa, inet_ntop, AF_INET6
from struct import unpack

import aiofiles
import aiomysql
import bencoder
import chardet
import pylru
import pymysql
import toml
from mala import get_metadata

import base_sql

with open('config.toml', 'r') as f:
    cfg = toml.load(f)

connect_dict = {
    'host': cfg['mysql']['host'],
    'port': cfg['mysql']['port'],
    'user': cfg['mysql']['user'],
    'password': cfg['mysql']['password'],
    'db': cfg['mysql']['db'],
    'charset': cfg['mysql']['charset']
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
    # 26Bytes = 20Bytes NodeID + 4 Bytes IPv4 Address + 2Bytes Port
    # 38Bytes = 20Bytes NodeID + 16Bytes IPv6 Address + 2Bytes Port
    if (length % 26) != 0 and (length % 38) != 0:
        return

    if (length % 26) == 0:
        for i in range(0, length, 26):
            nid = nodes[i:i + 20]
            ip = inet_ntoa(nodes[i + 20:i + 24])
            port = unpack('!H', nodes[i + 24:i + 26])[0]
            yield nid, ip, port
    else:
        for i in range(0, length, 38):
            nid = nodes[i:i + 20]
            ip = inet_ntop(AF_INET6, nodes[i + 20:i + 36])
            port = unpack('!H', nodes[i + 36:i + 38])[0]
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


def get_meta_hash(meta_info):
    return hashlib.sha1(bencoder.bencode(meta_info)).hexdigest().upper()


def sizeof_fmt(num, suffix="B"):
    # https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Y{suffix}"


BOOTSTRAP_NODES = (
    ('router.bittorrent.com', 6881),
    ('dht.transmissionbt.com', 6881),
    ('router.utorrent.com', 6881),
    ('tracker.openbittorrent.com', 80),
    ('tracker.opentrackr.org', 1337)
)


class Crawler(asyncio.DatagramProtocol):
    '''
    This class' implementation is from https://github.com/whtsky/maga/blob/master/maga.py
    '''

    def __init__(self, loop=None, bootstrap_nodes=BOOTSTRAP_NODES, interval=3):
        self.node_id = random_node_id()
        self.transport = None
        self.loop = loop or asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.connection_pool = self.loop.run_until_complete(aiomysql.create_pool(loop=self.loop, **connect_dict))
        self.database_batch = 48
        self.queue_size = 2048
        self.announce_queue = asyncio.Queue(self.queue_size)
        self.peers_queue = asyncio.Queue(self.queue_size)
        self.node_queue = asyncio.Queue(self.queue_size)
        self.max_fetch_task = 128
        self.max_database_semaphore = int(1.5 * self.max_fetch_task)
        self.fetch_metainfo_semaphore = asyncio.Semaphore(self.max_fetch_task)
        self.database_semaphore = asyncio.Semaphore(self.max_database_semaphore)
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
            for node in self.bootstrap_nodes:
                self.find_peers(addr=node)

    def run(self, port=6881):
        coroutine = self.loop.create_datagram_endpoint(
            lambda: self, local_addr=('::', port)
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
        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        asyncio.ensure_future(self.auto_get_metainfo(), loop=self.loop)
        asyncio.ensure_future(self.handle_announce_queue(), loop=self.loop)
        asyncio.ensure_future(self.info_logger(), loop=self.loop)
        self.loop.run_forever()
        self.loop.close()

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.__running = False
        self.transport.close()

    def send_message(self, data, addr):
        data.setdefault('t', b'tt')
        self.transport.sendto(bencoder.bencode(data), addr)

    def find_node(self, addr, target_node_id=None):
        if not target_node_id:
            if self.node_queue.empty():
                target_node_id = random_node_id()
            else:
                target_node_id = self.node_queue.get_nowait()
        self.send_message({
            't': 'aa',
            'y': 'q',
            'q': 'find_node',
            'a': {
                'id': self.node_id,
                'target': target_node_id
            }
        }, addr=addr)

    def find_peers(self, addr, target_peer_id=None):
        if not target_peer_id:
            if self.peers_queue.empty():
                target_peer_id = random_node_id()
            else:
                target_peer_id = self.peers_queue.get_nowait()
        self.send_message({
            't': 'aa',
            'y': 'q',
            'q': 'get_peers',
            'a': {
                'id': self.node_id,
                'info_hash': target_peer_id
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
                't': msg[b't'],
                'y': 'e',
                'e': [202, 'Server Error']
            }, addr=addr)
            raise e

    def handle_message(self, msg, addr):
        msg_type = msg.get(b'y', b'e')
        if msg_type == b'e':
            return
        if msg_type == b'r':
            return self.handle_response(msg, addr=addr)
        if msg_type == b'q':
            return asyncio.ensure_future(
                self.handle_query(msg, addr=addr), loop=self.loop
            )

    def handle_response(self, msg, addr):
        try:
            args = msg[b'r']
            node_id = args[b'id']
        except:
            return
        if b'nodes' in args:
            for node_id, ip, port in split_nodes(args[b'nodes']):
                self.ping(addr=(ip, port))
        if b'nodes6' in args:
            for node_id, ip, port in split_nodes(args[b'nodes6']):
                self.ping(addr=(ip, port))

    async def handle_query(self, msg, addr):
        try:
            args = msg[b'a']
            node_id = args[b'id']
            query_type = msg[b'q']
        except:
            return
        if node_id == self.node_id:
            return
        if query_type == b'get_peers':
            infohash = args[b'info_hash']
            infohash = proper_infohash(infohash)
            token = infohash[:2]
            self.send_message({
                't': msg[b't'],
                'y': 'r',
                'r': {
                    'id': self.node_id,
                    'nodes': '',
                    'token': token
                }
            }, addr=addr)
            await self.handle_get_peers(infohash, addr)
        elif query_type == b'announce_peer':
            infohash = args[b'info_hash']
            target_id = msg[b't']
            self.send_message({
                't': target_id,
                'y': 'r',
                'r': {
                    'id': self.node_id
                }
            }, addr=addr)
            peer_addr = [addr[0], addr[1]]
            try:
                peer_addr[1] = args[b'port']
            except KeyError:
                pass
            await self.handle_announce_peer(proper_infohash(infohash), addr, peer_addr)
        elif query_type == b'find_node':
            target_id = msg[b't']
            target_node_id = args[b'target']
            self.send_message({
                't': target_id,
                'y': 'r',
                'r': {
                    'id': self.node_id,
                    'nodes': ''
                }
            }, addr=addr)
            await self.node_queue.put(target_node_id)
        elif query_type == b'ping':
            self.send_message({
                't': 'tt',
                'y': 'r',
                'r': {
                    'id': self.node_id
                }
            }, addr=addr)
        self.find_node(addr=addr)

    def ping(self, addr):
        self.send_message({
            'y': 'q',
            't': 'pg',
            'q': 'ping',
            'a': {
                'id': self.node_id
            }
        }, addr=addr)

    async def handle_get_peers(self, infohash, addr):
        # logging.info(
        #    'Receive get peers message from DHT {}. Infohash: {}.'.format(
        #        addr, infohash
        #    )
        # )
        if len(infohash) != 40:
           return
        await self.peers_queue.put(infohash)

    async def handle_announce_peer(self, infohash, addr, peer_addr):
        # logging.info(
        #    'Receive announce peer message from DHT {}. Infohash: {}. Peer address:{}'.format(
        #        addr, infohash, peer_addr
        #    )
        # )
        if len(infohash) != 40:
            return
        await self.announce_queue.put((infohash, addr, peer_addr))

    async def handle_announce_queue(self):
        while self.__running:
            if self.announce_queue.empty():
                await asyncio.sleep(self.interval)
                continue
            peer_list = []
            while not self.announce_queue.empty():
                try:
                    peer_list.append(self.announce_queue.get_nowait())
                except asyncio.QueueEmpty as e:
                    break
            async with self.database_semaphore:
                async with self.connection_pool.acquire() as connect:
                    try:
                        cursor = await connect.cursor()
                        for peer_info in peer_list:
                            await cursor.execute(base_sql.insert_into_announce_queue.format(
                                info_hash=peer_info[0],
                                ip_addr=peer_info[2][0],
                                port=peer_info[2][1]))
                        await connect.commit()
                    except pymysql.err.OperationalError as e:
                        # Dead lock
                        await connect.rollback()
                    await cursor.close()

    async def get_metainfo(self, infohash, addr):
        timeout = False
        async with self.fetch_metainfo_semaphore:
            filename = f'{cfg["torrent"]["save_path"]}{os.sep}{infohash.lower()}.torrent'
            if len(glob.glob(filename)) != 0:
                return
            try:
                metainfo = await asyncio.wait_for(
                    get_metadata(
                        infohash, addr[0], addr[1]
                    ),
                    timeout=self.interval * 20)
            except:
                timeout = True
            if timeout:
                # Query timeout
                await self.remove_info_from_queue(infohash, addr[0], addr[1])
            elif isinstance(metainfo, bool) and metainfo is False:
                # Mala return False
                await self.remove_info_from_queue(infohash, addr[0], addr[1])
            elif metainfo is None:
                # Peer return None
                await self.remove_info_from_queue(infohash, addr[0], addr[1])
            elif metainfo is not None and infohash != get_meta_hash(metainfo):
                # Metainfo's hash not equal query's hash
                await self.remove_info_from_queue(infohash, addr[0], addr[1])
            else:
                # Other conditions
                name = get_filename(metainfo)
                size = get_file_size(metainfo)
                logging.info(f'Hash: {infohash}. Name: {name}. Size: {sizeof_fmt(size)}')
                file_content = bencoder.bencode({b'info': metainfo})
                async with aiofiles.open(filename, mode='wb') as f:
                    await f.write(file_content)
                async with self.database_semaphore:
                    async with self.connection_pool.acquire() as connect:
                        cursor = await connect.cursor()
                        try:
                            await cursor.execute(base_sql.insert_into_torrent.format(
                                name=aiomysql.escape_string(name),
                                info_hash=infohash,
                                size=size))
                        except pymysql.err.IntegrityError as e:
                            # Duplicated primary key
                            await connect.rollback()
                        finally:
                            await cursor.execute(base_sql.remove_from_announce_queue.format(info_hash=infohash, ip_addr=addr[0], port=addr[1]))
                            await connect.commit()
                        await cursor.close()

    async def remove_info_from_queue(self, infohash, ip_addr, port):
        async with self.database_semaphore:
            async with self.connection_pool.acquire() as connect:
                cursor = await connect.cursor()
                try:
                    await cursor.execute(base_sql.remove_from_announce_queue.format(info_hash=infohash, ip_addr=ip_addr, port=port))
                    await connect.commit()
                except pymysql.err.OperationalError as e:
                    # Dead lock
                    await connect.rollback()
                await cursor.close()

    async def auto_get_metainfo(self):
        async with self.database_semaphore:
            async with self.connection_pool.acquire() as connect:
                # Clean locked, too old, stored info in announce queue
                cursor = await connect.cursor()
                await cursor.execute(base_sql.clean_announce_queue)
                await cursor.execute(base_sql.remove_too_old_announce_queue)
                await cursor.execute(base_sql.remove_stored_in_announce_queue)
                await connect.commit()
                await cursor.close()
        while self.__running:
            async with self.database_semaphore:
                async with self.connection_pool.acquire() as connect:
                    cursor = await connect.cursor()
                    await cursor.execute(base_sql.get_announce_queue_size)
                    (announce_queue_size,) = await cursor.fetchone()
                    await cursor.execute(base_sql.announce_queue_fetching_count)
                    (announce_queue_fetching_count,) = await cursor.fetchone()
                    if announce_queue_size == 0:
                        await asyncio.sleep(self.interval)
                        continue
                    elif announce_queue_fetching_count >= self.max_fetch_task:
                        limit_factor = max(16, math.ceil(announce_queue_fetching_count / self.max_fetch_task))
                        delay_time = self.interval * limit_factor
                        await asyncio.sleep(delay_time)
                    limit = min(self.database_batch, announce_queue_size)
                    try:
                        await cursor.execute(base_sql.get_batch_in_announce_queue.format(limit=limit))
                        data_list = await cursor.fetchall()
                        await connect.commit()
                        for data in data_list:
                            if data is None:
                                continue
                            infohash = data[0]
                            peer_addr = (data[1], data[2])
                            await cursor.execute(base_sql.set_lock.format(info_hash=infohash, ip_addr=peer_addr[0], port=peer_addr[1]))
                            await connect.commit()
                            asyncio.ensure_future(self.get_metainfo(infohash, peer_addr), loop=self.loop)
                        await connect.commit()
                    except pymysql.err.OperationalError as e:
                        # Dead lock
                        await connect.rollback()
                    await cursor.close()
                await asyncio.sleep(self.interval)

    async def handler(self, infohash, addr):
        pass

    async def info_logger(self):
        while self.__running:
            async with self.database_semaphore:
                async with self.connection_pool.acquire() as connect:
                    cursor = await connect.cursor()
                    await cursor.execute(base_sql.torrent_count)
                    (torrent_count,) = await cursor.fetchone()
                    await cursor.execute(base_sql.announce_queue_fetching_count)
                    (announce_queue_fetching_count,) = await cursor.fetchone()
                    await cursor.execute(base_sql.announce_queue_pending_count)
                    (announce_queue_pending_count,) = await cursor.fetchone()
                    await connect.commit()
                    await cursor.close()
            logging.info(
                f'{torrent_count} torrent(s) in database, '
                f'Fetching: {announce_queue_fetching_count}, '
                f'Pending: {announce_queue_pending_count}.'
            )
            await asyncio.sleep(self.interval * 10)


if __name__ == '__main__':
    crawl = Crawler()
    crawl.run()
