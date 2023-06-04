--ISDS 570 Group Project
--Create eod_quotes; import eod.csv
CREATE TABLE public.eod_quotes
(
    ticker character varying(16) COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    adj_open real,
    adj_high real,
    adj_low real,
    adj_close real,
    adj_volume numeric,
    CONSTRAINT eod_quotes_pkey PRIMARY KEY (ticker, date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.eod_quotes
    OWNER to postgres;
	

--Create eod view 2016-2021
CREATE OR REPLACE VIEW public.v_eod_quotes_2016_2021 AS
 SELECT eod_quotes.ticker,
    eod_quotes.date,
    eod_quotes.adj_close
   FROM eod_quotes
  WHERE eod_quotes.date >= '2016-01-01'::date AND eod_quotes.date <= '2021-03-26'::date;

ALTER TABLE public.v_eod_quotes_2016_2021
    OWNER TO postgres;
	
--Create eod_indices table; import data from 2016-01-01 to 2021-03-26
CREATE TABLE public.eod_indices
(
    symbol character varying(16) COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    open real,
    high real,
    low real,
    close real,
    adj_close real,
    volume double precision,
    CONSTRAINT eod_indices_pkey PRIMARY KEY (symbol, date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.eod_indices
    OWNER to postgres;
	
	
--Create eod_indices view 2016-2021
CREATE OR REPLACE VIEW public.v_eod_indices_2016_2021 AS
 SELECT eod_indices.symbol,
    eod_indices.date,
    eod_indices.adj_close
   FROM eod_indices
   WHERE eod_indices.date >= '2016-01-01'::date AND eod_indices.date <= '2021-03-26'::date;

   
ALTER TABLE public.v_eod_indices_2016_2021
    OWNER TO postgres;
	
--Create custom calendar; import custom calendar with 2016-2021 data
CREATE TABLE public.custom_calendar
(
    date date NOT NULL,
    y integer,
    m integer,
    d integer,
    dow character varying(3) COLLATE pg_catalog."default",
    trading smallint,
    CONSTRAINT custom_calendar_pkey PRIMARY KEY (date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.custom_calendar
    OWNER to postgres;
	
--Alter table with eom and prev trading day
ALTER TABLE public.custom_calendar
    ADD COLUMN eom smallint;

ALTER TABLE public.custom_calendar
    ADD COLUMN prev_trading_day date;

--Update prev trading day
UPDATE custom_calendar
SET prev_trading_day = PTD.ptd
FROM (SELECT date, (SELECT MAX(CC.date) FROM custom_calendar CC WHERE CC.trading=1 AND CC.date<custom_calendar.date) ptd FROM custom_calendar) PTD
WHERE custom_calendar.date = PTD.date;

--Insert prev trading day
INSERT INTO custom_calendar VALUES('2015-12-31',2015,12,31,'Thu',1,1,NULL);

--Update eom
UPDATE custom_calendar
SET eom = EOMI.endofm
FROM (SELECT CC.date,CASE WHEN EOM.y IS NULL THEN 0 ELSE 1 END endofm FROM custom_calendar CC LEFT JOIN
(SELECT y,m,MAX(d) lastd FROM custom_calendar WHERE trading=1 GROUP by y,m) EOM
ON CC.y=EOM.y AND CC.m=EOM.m AND CC.d=EOM.lastd) EOMI
WHERE custom_calendar.date = EOMI.date;
	
	
-- Store the excluded tickers (less than 99% complete in a table)
SELECT ticker, 'More than 1% missing' as reason
INTO exclusions_2016_2021
FROM v_eod_quotes_2016_2021
GROUP BY ticker
HAVING count(*)::real/(SELECT COUNT(*) FROM custom_calendar WHERE trading=1 AND date BETWEEN '2016-01-01' AND '2021-03-26')::real<0.99;

ALTER TABLE public.exclusions_2016_2021
    ADD CONSTRAINT exclusions_2016_2021_pkey PRIMARY KEY (ticker);
	
	
--Create view
CREATE OR REPLACE VIEW public.v_eod_2016_2021 AS
 SELECT v_eod_indices_2016_2021.symbol,
    v_eod_indices_2016_2021.date,
    v_eod_indices_2016_2021.adj_close
   FROM v_eod_indices_2016_2021
  WHERE NOT (v_eod_indices_2016_2021.symbol::text IN ( SELECT DISTINCT exclusions_2016_2021.ticker
           FROM exclusions_2016_2021))
UNION
 SELECT v_eod_quotes_2016_2021.ticker AS symbol,
    v_eod_quotes_2016_2021.date,
    v_eod_quotes_2016_2021.adj_close
   FROM v_eod_quotes_2016_2021
  WHERE NOT (v_eod_quotes_2016_2021.ticker::text IN ( SELECT DISTINCT exclusions_2016_2021.ticker
           FROM exclusions_2016_2021));

ALTER TABLE public.v_eod_2016_2021
    OWNER TO postgres;
	
--Create eod materialized view
CREATE MATERIALIZED VIEW public.mv_eod_2016_2021
TABLESPACE pg_default
AS
 SELECT v_eod_indices_2016_2021.symbol,
    v_eod_indices_2016_2021.date,
    v_eod_indices_2016_2021.adj_close
   FROM v_eod_indices_2016_2021
  WHERE NOT (v_eod_indices_2016_2021.symbol::text IN ( SELECT DISTINCT exclusions_2016_2021.ticker
           FROM exclusions_2016_2021))
UNION
 SELECT v_eod_quotes_2016_2021.ticker AS symbol,
    v_eod_quotes_2016_2021.date,
    v_eod_quotes_2016_2021.adj_close
   FROM v_eod_quotes_2016_2021
  WHERE NOT (v_eod_quotes_2016_2021.ticker::text IN ( SELECT DISTINCT exclusions_2016_2021.ticker
           FROM exclusions_2016_2021))
WITH NO DATA;

ALTER TABLE public.mv_eod_2016_2021
    OWNER TO postgres;
	

--Refresh with data
REFRESH MATERIALIZED VIEW mv_eod_2016_2021 WITH DATA;
	
	
--Create ret materialized view
CREATE MATERIALIZED VIEW public.mv_ret_2016_2021
TABLESPACE pg_default
AS
 SELECT eod.symbol,
    eod.date,
    eod.adj_close / prev_eod.adj_close - 1.0::double precision AS ret
   FROM mv_eod_2016_2021 eod
     JOIN custom_calendar cc ON eod.date = cc.date
     JOIN mv_eod_2016_2021 prev_eod ON prev_eod.symbol::text = eod.symbol::text AND prev_eod.date = cc.prev_trading_day
WITH NO DATA;

ALTER TABLE public.mv_ret_2016_2021
    OWNER TO postgres;

--Refresh with data
REFRESH MATERIALIZED VIEW mv_ret_2016_2021 WITH DATA;

--Insert into exclusions
INSERT INTO exclusions_2016_2021
SELECT DISTINCT symbol, 'Return higher than 100%' as reason FROM mv_ret_2016_2021 WHERE ret>1.0;

--Refresh mv eod and mv ret
REFRESH MATERIALIZED VIEW mv_eod_2016_2021 WITH DATA;
REFRESH MATERIALIZED VIEW mv_ret_2016_2021 WITH DATA;

--Export Daily Prices 2016-2021
SELECT PR.* 
INTO export_daily_prices_2016_2021
FROM custom_calendar CC LEFT JOIN mv_eod_2016_2021 PR ON CC.date=PR.date
WHERE CC.trading=1;

	
CREATE USER groupprojectreader WITH
	LOGIN
	NOSUPERUSER
	NOCREATEDB
	NOCREATEROLE
	INHERIT
	NOREPLICATION
	CONNECTION LIMIT -1
	PASSWORD 'read123';
*/

-- Grant read rights (on existing tables and views)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO groupprojectreader;

-- Grant read rights (for future tables and views)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
   GRANT SELECT ON TABLES TO groupprojectreader;
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	