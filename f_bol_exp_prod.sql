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

--build departures from US ports
CREATE TEMP TABLE t_dep AS
(
                SELECT
                    imo,
                    a.poi AS poi_depart,
                    date_depart
                FROM t_asvt_arrival a
                JOIN as_vessel_exp b    ON b.vessel = a.vessel
                JOIN as_poi c ON c.poi = a.poi
                JOIN port d ON d.code = c.port
                WHERE
                    c.cmdty <> '' AND
                    d.country = 'UNITED STATES' AND
                    c.type <> 'Lightering Zone' AND
                    a.draught_depart > a.draught_arrive
);
/*
Note:
(1) draught_changes 1) only use '>', t_dep has 36410 rows
                    2) '>=', t_dep has 62672 rows.
--> we may lose some departures if zero draft changes are not included,
since they are likely to change after departing the port for a while.
*/


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
