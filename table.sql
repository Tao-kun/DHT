create table torrent
(
    name      varchar(2047) null,
    info_hash varchar(40)   not null
        primary key,
    size      bigint        null
);

create table announce_queue
(
    info_hash   varchar(40)                           not null,
    ip_addr     varchar(100)                          not null,
    port        int                                   not null,
    `lock`      int       default 0                   null,
    insert_time timestamp default current_timestamp() not null,
    primary key (info_hash, ip_addr, port)
);

create table crawler_statistic
(
    totoal_count   int                                   not null,
    fetching_count int                                   not null,
    pending_count  int                                   not null,
    log_time       timestamp default current_timestamp() not null primary key
);