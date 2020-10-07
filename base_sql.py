insert_into_announce_queue = """insert ignore into announce_queue (`info_hash`, `ip_addr`, `port`) 
select * from (select '{info_hash}', '{ip_addr}', {port}) as tmp 
where not exists(select info_hash from torrent where info_hash = '{info_hash}')
on duplicate key update insert_time=current_timestamp();"""

get_size_in_announce_queue = """select count(info_hash) from announce_queue where `lock` != 1;"""

get_batch_in_announce_queue = """select info_hash, ip_addr, port from announce_queue 
where `lock` != {limit} order by insert_time desc limit 1 for update;"""

set_lock = """update announce_queue set `lock`=1 where info_hash = '{info_hash}';"""

insert_into_torrent = """insert into torrent (`name`, `info_hash`, `size`) values ('{name}', '{info_hash}', {size});"""

remove_from_announce_queue = """delete from announce_queue where `info_hash` = '{info_hash}';"""

clean_announce_queue = """delete from announce_queue where `lock` = 1;"""

torrent_count = """select count(info_hash) from torrent;"""

announce_queue_count = """select count(info_hash) from announce_queue where `lock` = 1;"""

torrent_exist = """select count(*) from torrent where `info_hash` = '{info_hash}';"""
