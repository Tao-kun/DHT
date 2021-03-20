import aiofiles
import aiomysql
import asyncio
import bencoder
import os

import pymysql
import base_sql
import crawler

read_semp = asyncio.Semaphore(64)
data_semp = asyncio.Semaphore(32)


async def _insert(connection_pool, filename):
    async with read_semp:
        async with aiofiles.open(filename, 'rb') as f:
            data = await f.read()
    try:
        metainfo = bencoder.bdecode2(data)[0][b'info']
    except:
        print('error: {}'.format(filename))
        return
    name = crawler.get_filename(metainfo)
    size = crawler.get_file_size(metainfo)
    meta_hash = crawler.get_meta_hash(metainfo)
    async with data_semp:
        async with connection_pool.acquire() as connect:
            cursor = await connect.cursor()
            await cursor.execute(base_sql.torrent_exist.format(info_hash=meta_hash))
            (data,) = await cursor.fetchone()
            await connect.commit()
            if data == 0:
                await cursor.execute(base_sql.insert_into_torrent.format(
                    name=pymysql.escape_string(name),
                    info_hash=meta_hash,
                    size=size))
                print('insert {} {} {}'.format(meta_hash, name, size))
            await connect.commit()
            await cursor.close()


async def _task(loop, connection_pool):
    while True:
        torrent_list = os.listdir(crawler.cfg.get('torrent', 'save_path'))
        for torrent_name in torrent_list:
            asyncio.ensure_future(_insert(connection_pool,
                                          '{}{}{}'.format(crawler.cfg.get('torrent', 'save_path'),
                                                          os.sep,
                                                          torrent_name)),
                                  loop=loop)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    connection_pool = loop.run_until_complete(aiomysql.create_pool(loop=loop, **crawler.connect_dict))
    loop.run_until_complete(_task(loop, connection_pool))
    try:
       loop.run_forever()
    except KeyBoardInterrupt:
        pass
    finally:
        connection_pool.close()
        loop.close()