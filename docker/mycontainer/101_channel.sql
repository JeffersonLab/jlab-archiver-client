-- channel100 is a scaler double floating point type
-- histmya0: Based on modified version of 100_channel.sql
-- Based on R123GMES.  Hostname and PV name changed.
INSERT INTO channels
(chan_id, name, type, adel, size, clip, active, request, alert, host, backup, ioc)
VALUES (101, 'channel101', 6, 0.01, 1, NULL, 1, 1, 0, 'mya', '6197335468200165376', NULL);


-- hstmya1: copied from similar PV in mycontainer.  Matches `show create` from mysql CLI tool
CREATE TABLE `table_101`
(
    `time` bigint(20)          NOT NULL,
    `code` tinyint(3) unsigned NOT NULL DEFAULT '0',
    `val1` float               NOT NULL DEFAULT '0',
    PRIMARY KEY (`time`)
);


-- hstmya1:
-- mysqldump -t -u myapi -p archive table_32092 --single-transaction --compact --where "time <= 409403584099123200 and time >= 409241234335334400" > /tmp/dump.sql
INSERT INTO `table_101`
VALUES (409252071447660840, 0, 0),
       (409252072521402621, 0, 6.996),
       (409252072789838067, 0, 7.93),
       (409252159217281673, 0, 7.755),
       (409253148843048959, 1, 0),
       (409253358329364789, 0, 7.755),
       (409359951810995172, 0, 0),
       (409359952347866063, 0, 1.979),
       (409359952508927330, 0, 7.501),
       (409359952607353660, 0, 7.754),
       (409372674062797368, 0, 0),
       (409372675127591301, 0, 4.973),
       (409372675404974595, 0, 7.753);
