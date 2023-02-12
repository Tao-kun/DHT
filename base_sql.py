insert_into_announce_queue = '''insert ignore into announce_queue (`info_hash`, `ip_addr`, `port`) 
select * from (select '{info_hash}', '{ip_addr}', {port}) as tmp 
where not exists (select t.`info_hash` from torrent t where `info_hash` = '{info_hash}')
on duplicate key update insert_time=current_timestamp();'''

get_announce_queue_size = '''select count(`info_hash`) from announce_queue where `lock` != 1;'''

get_batch_in_announce_queue = '''select `info_hash`, `ip_addr`, `port` from announce_queue 
where `lock` != 1 order by insert_time desc limit {limit} for update;'''

set_lock = '''update announce_queue set `lock`=1 where
`info_hash` = '{info_hash}' and `ip_addr` = '{ip_addr}' and `port` = {port};'''

insert_into_torrent = '''insert into torrent (`name`, `info_hash`, `size`)
values('{name}', '{info_hash}', {size});'''

remove_from_announce_queue = '''delete from announce_queue where
`info_hash` = '{info_hash}' and `ip_addr` = '{ip_addr}' and `port` = {port};'''

clean_announce_queue = '''delete from announce_queue where `lock` = 1;'''

torrent_count = '''select count(`info_hash`) from torrent;'''

announce_queue_fetching_count = '''select count(`info_hash`) from announce_queue where `lock` = 1;'''

announce_queue_pending_count = '''select count(`info_hash`) from announce_queue where `lock` = 0;'''

torrent_exist = '''select count(*) from torrent where `info_hash` = '{info_hash}';'''

remove_too_old_announce_queue = '''delete from announce_queue where `lock` = 0 and DATEDIFF(now(), insert_time) > 3;'''

remove_stored_in_announce_queue = '''delete from announce_queue aq where exists
(select t.`info_hash` from torrent t where t.`info_hash`=aq.`info_hash`);'''
