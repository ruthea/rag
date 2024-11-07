CREATE KEYSPACE movies WITH replication = {'class': 'NetworkTopologyStrategy', 'AWS_US_EAST_1': '3'}  AND durable_writes = true;

CREATE TABLE movies.movies (
    id int PRIMARY KEY,
    actors text,
    awards text,
    boxoffice text,
    country text,
    director text,
    dvd text,
    genre text,
    imdbid text,
    imdbrating float,
    imdbvotes text,
    language text,
    metascore int,
    plot text,
    poster text,
    production text,
    rated text,
    ratings_0_source text,
    ratings_0_value text,
    ratings_1_source text,
    ratings_1_value text,
    ratings_2_source text,
    ratings_2_value text,
    released text,
    response text,
    runtime text,
    title text,
    type text,
    website text,
    writer text,
    year int
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE' and 
    cdc = {'delta': 'full', 'enabled': 'true', 'postimage': 'false', 'preimage': 'false', 'ttl': '86400'};