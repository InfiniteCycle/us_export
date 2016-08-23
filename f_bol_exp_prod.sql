-- Function: f_bol_exp_prod()

-- DROP FUNCTION f_bol_exp_prod();

-- SYNTHESIZE EXPORT BOL RECORDS
SET CLIENT_MIN_MESSAGES = WARNING;
DROP TABLE IF EXISTS
                t_dm,
                t_dep,
                t_last_dep,
                t_last_dep_1,
                t_arr,
                t_arr_pc,
                t_first_arr,
                t_first_arr_1,
                t_bol_exp,
                t_bol_exp_1,
                t_max_filing_date;

DROP TABLE IF EXISTS t_asvt_encounter;
CREATE TEMP TABLE t_asvt_encounter AS
(
                SELECT * FROM mv_asvt_encounter
);
DELETE FROM t_asvt_encounter
WHERE
                vessel1 IN (SELECT vessel FROM as_vessel_exp WHERE imo IN (SELECT imo FROM tanker WHERE type ILIKE '%lpg%')) AND
                vessel2 NOT IN (SELECT vessel FROM as_vessel_exp WHERE imo IN (SELECT imo FROM tanker WHERE type ILIKE '%lpg%'));
DELETE FROM t_asvt_encounter
WHERE
                vessel2 IN (SELECT vessel FROM as_vessel_exp WHERE imo IN (SELECT imo FROM tanker WHERE type ILIKE '%lpg%')) AND
                vessel1 NOT IN (SELECT vessel FROM as_vessel_exp WHERE imo IN (SELECT imo FROM tanker WHERE type ILIKE '%lpg%'));

DROP TABLE IF EXISTS t_asvt_arrival;
CREATE TEMP TABLE t_asvt_arrival AS
(
                SELECT * FROM mv_asvt_arrival WHERE poi NOT IN (SELECT poi FROM as_poi WHERE type = 'Lightering Zone' AND poi IN (104142,105016))
                UNION
                select a.* from mv_asvt_arrival a
                join t_asvt_encounter b on
                                b.vessel1 = a.vessel and
                                b.vessel2 IN (select vessel from as_vessel_exp where imo in (select imo from tanker where (status = 'In Service' OR status IS NULL) AND type NOT IN ('Inland tanker'))) and
                                b.date_start >= a.date_arrive - interval '1 hour' and
                                b.date_start <= a.date_depart + interval '1 hour'
                where a.poi in (select poi from as_poi where type = 'Lightering Zone' AND poi IN (104142,105016))
);
CREATE INDEX t_asvt_arrival_date_arrive_idx ON t_asvt_arrival (date_arrive);
CREATE INDEX t_asvt_arrival_date_depart_idx ON t_asvt_arrival (date_depart);
CREATE INDEX t_asvt_arrival_vessel_date_arrive_idx ON t_asvt_arrival (vessel, date_arrive);
CREATE INDEX t_asvt_arrival_vessel_date_depart_idx ON t_asvt_arrival (vessel, date_depart);

--remove non-p poi & LPG tanker in arrivals.

DELETE FROM t_asvt_arrival WHERE vessel IN (
 SELECT vessel FROM as_vessel_exp WHERE imo IN (SELECT imo FROM tanker WHERE type ILIKE '%lpg%'))
      AND poi NOT IN (SELECT poi FROM poi_dir WHERE cmdty = 'P');
--combine bill data with data from 1401

CREATE TEMP TABLE t_max_filing_date AS
(
SELECT upper(btrim(vessel_name)) AS vessel,max(filing_date) AS max_filing_date FROM cbp.cf1401 GROUP BY upper(btrim(vessel_name))
);

CREATE TEMP TABLE t_dm AS
(
                SELECT DISTINCT
                                a.date                                                   ::DATE                   AS date,
                                COALESCE(x.imo, b.imo)                               ::INT                      AS imo,
                                COALESCE(c.product_code, '')    ::BPCHAR             AS grade,
                                UPPER(a.grade)                                                ::BPCHAR             AS description,
                                COALESCE(d.parent_code, '')     ::BPCHAR             AS shipper,
                                COALESCE(e.parent_code, '')     ::BPCHAR             AS consignee,
                                COALESCE(f.parent_code, '')      ::BPCHAR             AS notification,
                                COALESCE(g.lo_city_code, '')      ::BPCHAR             AS ld_city_decl,
                                COALESCE(g.lo_country_code, '') ::BPCHAR         AS ld_country_decl,
                                g.code                                                  ::INT                      AS port_custom,
                                COALESCE(h.lo_city_code, '')      ::BPCHAR             AS dis_city_decl,
                                COALESCE(h.lo_country_code, '')             ::BPCHAR             AS dis_country_decl,
                                COALESCE
                                (
                    a.bbls,
                    a.weight_mt * c.t_to_b,
                    0
                )                       ::INT                      AS bbls,
                                COALESCE
                                (
                    a.weight_mt,
                    a.bbls / nullif(c.t_to_b, 0),
                    0
                )         ::NUMERIC         AS weight_mt,
                                a.update_time                                  ::TIMESTAMP    AS update_time,
                                a.file
                FROM sa.dm_exp_process a
                LEFT JOIN t_max_filing_date a1 ON  a1.vessel = upper(btrim(a.vessel))
                LEFT JOIN
                (
                                SELECT
                                                a.vessel_name,
                                                a.imo,
                                                a.vessel_type,
                                                a.filing_date
                                FROM cbp.cf1401 a
                                JOIN
                                (
                                                SELECT
                                                                imo,
                                                                max(filing_date) AS filing_date
                                                FROM cbp.cf1401
                                                GROUP BY imo
                                )b ON
                                                b.imo = a.imo AND
                                                b.filing_date = a.filing_date
                )b ON
                                 upper(btrim(b.vessel_name)) = upper(btrim(a.vessel)) AND
                                b.vessel_type IN ('111','112','140','150','210','229')
                                --and b.filing_date = (select max(filing_date) from cbp.cf1401 where upper(btrim(vessel_name)) = upper(btrim(a.vessel)))
                                 and b.filing_date = a1.max_filing_date

                LEFT JOIN
                (
                                SELECT
                                                a.dm_name,
                                                a.product_code,
                                                coalesce(b.t_to_b, 0) as t_to_b
                                FROM dm_alias_prod a
                                LEFT JOIN cat_product b ON b.product_code = a.product_code
                                UNION
                                SELECT
                                                a.variety,
                                                a.crude_code,
                                                f_api2bpt(b.api) as t_to_b
                                FROM dm_alias_crude1 a
                                LEFT JOIN cat_crude b ON b.crude_code = a.crude_code
                ) c ON
                                upper(btrim(c.dm_name)) = upper(btrim(a.grade))
                LEFT JOIN dm_alias_co d                              ON upper(btrim(d.shipper_consignee))                = upper(btrim(a.shipper))
                LEFT JOIN dm_alias_co e                              ON upper(btrim(e.shipper_consignee))                = upper(btrim(a.consignee))
                LEFT JOIN dm_alias_co f                               ON upper(btrim(f.shipper_consignee))                 = upper(btrim(a.notification))
                LEFT JOIN lookup.dm_alias_port g           ON upper(btrim(g.name))                           = upper(btrim(a.port))
                LEFT JOIN lookup.dm_alias_port h           ON upper(btrim(h.name))                           = upper(btrim(a.destination))
                LEFT JOIN dm_exp_ximo x                                          ON x.file                                                              = a.file
);


/* New t_dep */
drop table if exists t_bill_dep;
create temp table t_bill_dep as (
with t_dep_v2 as (
SELECT imo,
       a.poi AS poi_depart,
       d.code port_code,
       d.name port_name,
       date_depart,
       c.lo_city_code,
       c.lo_country_code
FROM t_asvt_arrival a
JOIN as_vessel_exp b    ON b.vessel = a.vessel
JOIN as_poi c ON c.poi = a.poi
JOIN port d ON d.code = c.port
WHERE c.cmdty <> '' AND
       d.country = 'UNITED STATES' AND
       c.type <> 'Lightering Zone' AND
       a.draught_depart > a.draught_arrive
)

select a.imo,
       a.date bill_date,
       a.grade,
       a.description,
       a.ld_city_decl,
       a.ld_country_decl,
       a.port_custom,
       a.dis_city_decl,
       a.dis_country_decl,
       a.weight_mt,
       a.file,
       b.poi_depart,
       b.date_depart,
       b.port_code,
       b.port_name,
       b.lo_country_code,
       b.lo_city_code
from t_dm a
left join t_dep_v2 b on a.imo = b.imo
);

/*
1. Based on t_dm table, left join the table t_dep,
every bills will have a potential corresponding departure.

2. Add draught information in the t_dep_v2 table.
*/

drop table if exists t_bill_dep;
create temp table t_bill_dep as (
with t_dep_v2 as (
SELECT imo,
       a.poi AS poi_depart,
       d.code port_code,
       d.name port_name,
       date_depart,
       c.lo_city_code,
       c.lo_country_code,
       draught_arrive,
       draught_depart
FROM t_asvt_arrival a
JOIN as_vessel_exp b    ON b.vessel = a.vessel
JOIN as_poi c ON c.poi = a.poi
JOIN port d ON d.code = c.port
WHERE c.cmdty <> '' AND
       d.country = 'UNITED STATES' AND
       c.type <> 'Lightering Zone' AND
       a.draught_depart >= a.draught_arrive
)

select a.imo,
       a.date bill_date,
       a.grade,
       a.description,
       a.ld_city_decl,
       a.ld_country_decl,
       a.port_custom,
       a.dis_city_decl,
       a.dis_country_decl,
       a.weight_mt,
       a.file,
       b.poi_depart,
       b.date_depart,
       b.port_code,
       b.port_name,
       b.lo_country_code,
       b.lo_city_code,
       b.draught_arrive,
       b.draught_depart
from t_dm a
left join t_dep_v2 b on a.imo = b.imo
)
;


/*
1. Add an extra column: poi_cmdty into t_bill_dep;

This is used to make comparison with t_bill_dep_cmdty table,
which will filter out all the asvt_arrival records with poi
that could not handle reported commodity.
*/
drop table if exists t_bill_dep_v2;
create temp table t_bill_dep_v2 as (
with t0 as (
(select a.*,
    b.cd_report
from t_bill_dep a
left join cat_product b on a.grade = b.product_code
where grade not in (select distinct crude_code from cat_crude)
)
union all
(select a.*,
       'CRUDE' cd_report
from t_bill_dep a
where grade in (select distinct crude_code from cat_crude)
)),
t1 as (
select t0.*,
       b.cmdty
from t0
left join lookup.cmdty_cat_product b on t0.cd_report = b.cd_report
),
t_bill_dep_v2 as (
select t1.*,
       b.cmdty poi_cmdty
from t1
left join as_poi b on t1.poi_depart = b.poi
),
t_dep_null as (
select * from t_bill_dep_v2 a
where poi_depart is null
),
t_dep_fill_null as (
select a.imo,
       a.bill_date,
       a.grade,
       a.description,
       a.ld_city_decl,
       a.ld_country_decl,
       a.port_custom,
       a.dis_city_decl,
       a.dis_country_decl,
       a.weight_mt,
       a.file,
       b.poi poi_depart,
       b.date_depart,
       c.port port_code,
       d.name port_name,
       c.lo_country_code,
       c.lo_city_code,
       0 draught_arrive,
       0 draught_depart,
       a.cd_report,
       a.cmdty,
       c.cmdty poi_cmdty
from t_dep_null a
left join as_exp_x b on a.file = b.file
left join as_poi c on c.poi = b.poi
left join port d on c.port = d.code
)

select * from t_bill_dep_v2
union ALL
select * from t_dep_fill_null
)

/* Only include asvt_arrival record whose poi can handle
the commodities on the vessel */
drop table if exists t_bill_dep_cmdty;
create temp table t_bill_dep_cmdty as (
with t0 as (
(select a.*,
    b.cd_report
from t_bill_dep a
left join cat_product b on a.grade = b.product_code
where grade not in (select distinct crude_code from cat_crude)
)
union all
(select a.*,
       'CRUDE' cd_report
from t_bill_dep a
where grade in (select distinct crude_code from cat_crude)
)),
t1 as (
select t0.*,
       b.cmdty
from t0
left join lookup.cmdty_cat_product b on t0.cd_report = b.cd_report
),
t_bill_dep_v2 as (
select * from t1
),
t_dep_null as (
select * from t_bill_dep_v2 a
where poi_depart is null
),
t_dep_fill_null as (
select a.imo,
       a.bill_date,
       a.grade,
       a.description,
       a.ld_city_decl,
       a.ld_country_decl,
       a.port_custom,
       a.dis_city_decl,
       a.dis_country_decl,
       a.weight_mt,
       a.file,
       b.poi poi_depart,
       b.date_depart,
       c.port port_code,
       d.name port_name,
       c.lo_country_code,
       c.lo_city_code,
       0 draught_arrive,
       0 draught_depart,
       a.cd_report,
       a.cmdty
from t_dep_null a
left join as_exp_x b on a.file = b.file
left join as_poi c on c.poi = b.poi
left join port d on c.port = d.code
),
t_combine as (
select * from t_bill_dep_v2
union ALL
select * from t_dep_fill_null
)

select a.*,
       b.loadunl
from t_combine a
join poi_dir b on a.poi_depart = b.poi and a.cmdty = b.cmdty
);



-- (1) poi must can handle the reported commodity.
-- (2) select record with smallest time difference.
-- (3) port city must match.
drop table if exists t_timediff_cmdty_city;
create temp table t_timediff_cmdty_city as (
with t0 as (
select *,
       round(extract(epoch from (date_depart - bill_date))/3600/24, 4) time_diff
from t_bill_dep_cmdty
where ld_city_decl = lo_city_code and ld_country_decl = lo_country_code
-- where port_custom = port_code
),
t1 as (
select file,
       min(abs(time_diff)) min_time_diff
from t0
group by imo,
         file
)

select a.* from t0 a, t1 b
where abs(time_diff) = b.min_time_diff and
      a.file = b.file
)
;

-- (1) poi must can handle the reported commodity.
-- (2) select record with smallest time difference.
-- (3) port code must match.
drop table if exists t_timediff_cmdty_port;
create temp table t_timediff_cmdty_port as (
with t0 as (
select *,
       round(extract(epoch from (date_depart - bill_date))/3600/24, 4) time_diff
from t_bill_dep_cmdty
-- where ld_city_decl = lo_city_code and ld_country_decl = lo_country_code
where port_custom = port_code
),
t1 as (
select file,
       min(abs(time_diff)) min_time_diff
from t0
group by file
)

select a.* from t0 a, t1 b
where abs(time_diff) = b.min_time_diff and
      a.file = b.file
)
;


-- (1) poi must can handle the reported commodity.
-- (2) select record with smallest time difference.
-- (3) port state must match.
drop table if exists t_timediff_cmdty_state;
create temp table t_timediff_cmdty_state as (
with t0 as (
select a.*,
       round(extract(epoch from (date_depart - bill_date))/3600/24, 4) time_diff,
       b.state bill_port_state,
       c.state port_state
from t_bill_dep_cmdty a
left join port b on a.port_custom = b.code
left join port c on a.port_code = c.code
where b.state = c.state
),
t1 as (
select file,
    min(abs(time_diff)) min_time_diff
from t0
group by file
),
t2 as (
select a.* from t0 a, t1 b
where abs(time_diff) = b.min_time_diff and
      a.file = b.file
)
select * from t2
);


-- (1) Just select record having smallest time difference.
drop table if exists t_timediff_min;
create temp table t_time_diff_min as (
with t0 as (
select *,
       round(extract(epoch from (date_depart - bill_date))/3600/24, 4) time_diff
from t_bill_dep_v2
-- where ld_city_decl = lo_city_code and ld_country_decl = lo_country_code
-- where port_custom = port_code
),
t1 as (
select file,
       min(abs(time_diff)) min_time_diff
from t0
group by file
)

select a.* from t0 a, t1 b
where abs(time_diff) = b.min_time_diff and
      a.file = b.file
)
;
















--build arrivals at non-US ports
CREATE TEMP TABLE t_arr AS
(
            SELECT
                b.imo,
                a.poi AS poi_arrive,
                date_arrive
            FROM t_asvt_arrival a
            JOIN as_vessel_exp b ON b.vessel = a.vessel
            JOIN as_poi c ON c.poi = a.poi
            JOIN port d ON d.code = c.port
            WHERE
                EXISTS (SELECT 1 FROM poi_dir WHERE poi = a.poi LIMIT 1) AND
                d.country <> 'UNITED STATES'
);
--build arrivals at non-US ports PC
CREATE TEMP TABLE t_arr_pc AS
(
            SELECT
                b.imo,
                a.poi AS poi_arrive,
                date_arrive
            FROM t_asvt_arrival a
            JOIN as_vessel_exp b    ON b.vessel = a.vessel
            JOIN as_poi c                     ON c.poi = a.poi
            JOIN port d                         ON d.code = c.port
            WHERE
                c.type <> 'Lightering Zone' AND
                EXISTS (SELECT 1 FROM poi_dir WHERE poi = a.poi LIMIT 1) AND
                d.country <> 'UNITED STATES'
);


--find the last departure after the bill date
CREATE TEMP TABLE t_last_dep AS
(
            SELECT
                a.imo,
                a.file,
                a.grade,
                b.poi_depart,
                a.date_depart
            FROM
            (
                SELECT
                    a.imo,
                    a.file,
                    a.grade,
                    max(date_depart) AS date_depart
                FROM t_dm a
                LEFT JOIN t_dep b ON
                                b.imo = a.imo AND
                                b.date_depart > a.date - INTERVAL '4 days' AND
                                b.date_depart < a.date + INTERVAL '4 days'
                GROUP BY a.imo, a.file, a.grade
            ) a
            LEFT JOIN t_dep b ON
                            b.imo = a.imo AND
                            b.date_depart = a.date_depart
);

/*
Note:
(1) +/- intervals: '4 days' is probable just an guess. If the interval is set too short,
records in t_dm may not have matched items in t_dep.
*/


--bring in manual departures
CREATE TEMP TABLE t_last_dep_1 AS
(
            SELECT
                a.imo,
                a.file,
                a.grade,
                COALESCE(b.poi, c.poi_depart) AS poi_depart,
                COALESCE(b.date_depart, c.date_depart) AS date_depart
            FROM t_dm a
            LEFT JOIN as_exp_x b ON b.file = a.file
            JOIN t_last_dep c ON c.file = a.file
);
/*
According to the COALESCE function, table as_exp_x seems more reliable ?
*/


--find the first arrival after the departure date
CREATE TEMP TABLE t_first_arr AS
(
        SELECT
            a.imo,
            a.file,
            a.poi_depart,
            a.date_depart,
            min(date_arrive) AS date_arrive
        FROM t_last_dep_1 a
        LEFT JOIN t_arr b ON
                        b.imo = a.imo AND
                        a.date_depart < b.date_arrive
        WHERE a.grade NOT ILIKE 'PC%'
                GROUP BY a.imo, a.poi_depart, a.date_depart, a.file
        UNION ALL
        SELECT
            a.imo,
            a.file,
            a.poi_depart,
            a.date_depart,
            min(date_arrive) AS date_arrive
        FROM t_last_dep_1 a
        LEFT JOIN t_arr_pc b ON
                        b.imo = a.imo AND
                        a.date_depart < b.date_arrive
        WHERE a.grade ILIKE 'PC%'
                GROUP BY
                                a.imo,
                                a.poi_depart,
                                a.date_depart,
                                a.file
);
CREATE TEMP TABLE t_first_arr_1 AS
(
            SELECT
                            a.imo,
                            a.file,
                           COALESCE(b.poi, c.poi_arrive) AS poi_arrive,
                           COALESCE(b.date_arrive, c.date_arrive) AS date_arrive
            FROM t_dm a
            LEFT JOIN as_exp_arr_x b            ON b.file = a.file
            LEFT JOIN
            (
                            SELECT
                                            a.imo,
                                            c.file,
                                            a.poi_arrive,
                                            a.date_arrive
                            FROM t_arr a
                            JOIN t_last_dep_1 b ON
                                            b.imo = a.imo AND
                                            a.date_arrive > b.date_depart
                            JOIN t_first_arr c ON
                                            c.imo = a.imo AND
                                            c.date_arrive = a.date_arrive
            )c ON c.file = a.file
);
--combine bill and ais data
CREATE TEMP TABLE t_bol_exp AS
(
            SELECT DISTINCT
                            a.*,
                            b.poi_depart,
                            b.date_depart,
                            c.poi_arrive,
                            c.date_arrive,
                            CURRENT_TIMESTAMP AS update_time_arr
            FROM t_dm a
            LEFT JOIN t_last_dep_1 b ON
                            b.imo = a.imo AND
                            b.file = a.file
            LEFT JOIN t_first_arr_1 c ON
                            c.imo = b.imo AND
                            c.file = b.file
            ORDER BY date DESC
);
--remove bad records from output table
DELETE FROM bol_exp_prod
WHERE file IN
(
    SELECT file FROM
    (
        SELECT
            date,
            imo,
            grade,
            description,
            shipper,
            consignee,
            notification,
            ld_city_decl,
            ld_country_decl,
            port_custom,
            dis_city_decl,
            dis_country_decl,
            bbls,
            weight_mt,
            update_time,
            file,
            poi_depart,
            date_depart,
            poi_arrive,
            date_arrive
        FROM   bol_exp_prod
        EXCEPT
        SELECT
            date,
            imo,
            grade,
            description,
            shipper,
            consignee,
            notification,
            ld_city_decl,
            ld_country_decl,
            port_custom,
            dis_city_decl,
            dis_country_decl,
            bbls,
            weight_mt,
            update_time,
            file,
            poi_depart,
            date_depart,
            poi_arrive,
            date_arrive
        FROM t_bol_exp
    ) alias
);

CREATE TEMP TABLE t_bol_exp_1 AS
(
    SELECT * FROM t_bol_exp WHERE file IN
    (
        SELECT file FROM
        (
            SELECT
                date,
                imo,
                grade,
                description,
                shipper,
                consignee,
                notification,
                ld_city_decl,
                ld_country_decl,
                port_custom,
                dis_city_decl,
                dis_country_decl,
                bbls,
                weight_mt,
                update_time,
                file,
                poi_depart,
                date_depart,
                poi_arrive,
                date_arrive
            FROM t_bol_exp
            EXCEPT
            SELECT
                date,
                imo,
                grade,
                description,
                shipper,
                consignee,
                notification,
                ld_city_decl,
                ld_country_decl,
                port_custom,
                dis_city_decl,
                dis_country_decl,
                bbls,
                weight_mt,
                update_time,
                file,
               poi_depart,
                date_depart,
                poi_arrive,
                date_arrive
            FROM   bol_exp_prod
        ) alias
    )
);

-- alert for null imo
DELETE FROM alert.bol_exp_prod_nullimo;
INSERT INTO alert.bol_exp_prod_nullimo
SELECT
    a.file,
    b.vessel,
    b.flag,
    b.grade,
    b.port,
    b.update_time
FROM t_bol_exp_1 a
JOIN sa.dm_exp_process b ON b.file = a.file
WHERE a.imo IS NULL;

DELETE FROM t_bol_exp_1 WHERE imo IS NULL;

DELETE FROM bol_exp_prod
WHERE file IN (SELECT file FROM t_bol_exp_1);

-- final output
INSERT INTO bol_exp_prod
SELECT * FROM t_bol_exp_1
WHERE
    date >= current_timestamp::date - interval '5 years' and
    date <= current_timestamp::date + interval '5 years' and
    file not in (select file FROM t_bol_exp_1 group by file having count(file) > 1)
order by file;

INSERT INTO bol_exp_prod_bad SELECT * FROM t_bol_exp_1 WHERE (date > current_timestamp::date + interval '5 years' or date < current_timestamp::date - interval '5 years');
