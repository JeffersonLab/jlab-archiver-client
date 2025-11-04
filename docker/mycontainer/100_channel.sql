-- channel100 is a scaler double floating point type
-- histmya0: based on [archive]> select * from channels were name = 'R121GMES';
-- Based on R121GMES.  Hostname and PV name changed.
INSERT INTO channels
(chan_id, name, type, adel, size, clip, active, request, alert, host, backup, ioc)
VALUES (100, 'channel100', 6, 0.01, 1, NULL, 1, 1, 0, 'mya', '6197335468200165376', NULL);


-- hstmya1: copied from similar PV in mycontainer.  Matches `show create` from mysql CLI tool
CREATE TABLE `table_100`
(
    `time` bigint(20)          NOT NULL,
    `code` tinyint(3) unsigned NOT NULL DEFAULT '0',
    `val1` float               NOT NULL DEFAULT '0',
    PRIMARY KEY (`time`)
);


-- hstmya1: mysqldump -t -u myapi -p archive table_32010 --single-transaction --compact --where "time <= 409403584099123200 and time >= 409241234335334400" > /tmp/dump.sql
-- Range maps to 2018-04-24 to 2018-05-01
-- First event changed to make it a no prior data event
INSERT INTO `table_100`
VALUES (409247435559442690, 16, 0),
       (409247436633184471, 0, 5.911),
       (409252159494664967, 0, 5.66),
       (409253148843048959, 1, 0),
       (409253333096363335, 0, 5.657),
       (409266995941939460, 0, 0),
       (409266997284116687, 0, 5.657),
       (409297793596205387, 0, 0),
       (409297794401511723, 0, 0.031),
       (409297794660999320, 0, 2.466),
       (409297794938382613, 0, 5.657),
       (409324820261319287, 0, 0),
       (409324821335061068, 0, 1.418),
       (409324821603496513, 0, 5.657),
       (409359003426969480, 0, 0),
       (409359004500711261, 0, 5.657),
       (409390302253516467, 0, 0),
       (409390303595693694, 0, 5.658);

