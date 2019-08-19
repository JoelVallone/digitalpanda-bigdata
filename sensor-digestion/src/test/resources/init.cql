CREATE KEYSPACE IF NOT EXISTS iot
WITH durable_writes = true
AND replication = { 'replication_factor' : 1, 'class' : 'SimpleStrategy' };


CREATE TABLE IF NOT EXISTS iot.sensor_measure_latest (
   location text,
   measure_type text,
   timestamp timestamp,
   value double,
   primary key((location), measure_type)
) WITH COMPACTION = {'class': 'LeveledCompactionStrategy',
                      'tombstone_compaction_interval': '14400'};


CREATE TABLE IF NOT EXISTS iot.sensor_measure_history_seconds (
    location text,
    time_block_id bigint,
    measure_type text,
    bucket int,
    timestamp timestamp,
    value double,
    primary key((location, time_block_id, measure_type, bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
          AND COMPACTION = {'class': 'TimeWindowCompactionStrategy',
                        'compaction_window_unit': 'DAYS',
                        'compaction_window_size': 7}
          AND default_time_to_live = 94608000; -- 3 * 365 * 24 * 3600


INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:59.928000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:58.927000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:57.926000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:56.926000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:55.926000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:54.925000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:53.926000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:52.926000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:09:51.926000+ 0000', 26.5);

INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'TEMPERATURE', 0, '2019-06-30 22:19:50.926000+ 0000', 300000.5);


INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'server-room', 2979, 'PRESSURE'   , 0, '2019-06-30 22:09:50.926000+ 0000', 600000.5);


INSERT INTO iot.sensor_measure_history_seconds (location, time_block_id, measure_type, bucket, timestamp, value) VALUES
    ( 'outdoor'      2979, 'TEMPERATURE', 0, '2019-06-30 22:09:50.926000+ 0000', 27.5);



INSERT INTO iot.sensor_measure_latest (location, measure_type, timestamp, value) VALUES
    ( 'server-room',     'PRESSURE' '2019-06-30 22:09:50.926000+ 0000', 966.5);

INSERT INTO iot.sensor_measure_latest (location, measure_type, timestamp, value) VALUES
    ( 'server-room',  'TEMPERATURE' '2019-06-30 22:09:50.926000+ 0000', 23.3);

INSERT INTO iot.sensor_measure_latest (location, measure_type, timestamp, value) VALUES
    ( 'outdoor',     'HUMIDITY' '2019-06-30 22:09:50.926000+ 0000', 75.33896);

INSERT INTO iot.sensor_measure_latest (location, measure_type, timestamp, value) VALUES
    ( 'outdoor',     'PRESSURE' '2019-06-30 22:09:50.926000+ 0000', 967.72356);

INSERT INTO iot.sensor_measure_latest (location, measure_type, timestamp, value) VALUES
    ( 'outdoor', 'TEMPERATURE', '2019-06-30 22:09:50.926000+ 0000', 23.7739);
