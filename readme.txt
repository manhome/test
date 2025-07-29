hk_sec_return.db
hk_sec_price.db

table: securities
table: sec_return (sec_id, price_date, open, high, low, close, volume, adj_close)
table: last_price (sec_id, price_date, open, high, low, close, volume, adj_close)

table: sec_price (sec_id, price_date, open, high, low, close, volume, adj_close)


CREATE TABLE securities (
    sec_id INTEGER NOT NULL,
    name NVARCHAR(50),
    ccy NVARCHAR(3),
    exch_yahoo NVARCHAR(5),
    id_yahoo NVARCHAR(15),
    PRIMARY KEY(sec_id)
);

CREATE TABLE sec_return (
    sec_id INTEGER NOT NULL,
    Date DATE NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    adj_close REAL,
    PRIMARY KEY(sec_id, Date)
);

CREATE TABLE last_price (
    sec_id INTEGER NOT NULL,
    Date DATE NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    PRIMARY KEY(sec_id)
);

CREATE TABLE last_volume (
    sec_id INTEGER NOT NULL,
    Date DATE NOT NULL,
    volume REAL,
    PRIMARY KEY(sec_id)
);

CREATE TABLE sec_price (
    sec_id INTEGER NOT NULL,
    Date DATE NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    adj_close REAL,
    PRIMARY KEY(sec_id, Date)
);

--------------------------------------------------------------------------------
DELETE FROM sec_price;

DELETE FROM last_price;
DELETE FROM last_volume;
DELETE FROM sec_return;

--------------------------------------------------------------------------------
SELECT a1.sec_id, a1.id_yahoo,
    IFNULL((SELECT MAX(a2.Date) FROM sec_return a2
        WHERE a2.sec_id = a1.sec_id
        AND a2.volume > 0), '1900-01-01') AS [Date]
FROM securities a1

SELECT s1.sec_id, 
    unixepoch('1970-01-01') AS [Date]
FROM securities s1
WHERE NOT EXISTS(SELECT 1 FROM sec_return a1 
    WHERE a1.sec_id = s1.sec_id)
UNION SELECT a1.sec_id, 
    MAX(a1.Date) AS [Date]
FROM sec_return a1 
WHERE a1.volume != 0
AND a1.Date < (
    SELECT MAX(a2.Date) FROM sec_return a2
    WHERE a2.sec_id = a1.sec_id
        AND a2.volume != 0
        AND a2.Date < ?)
GROUP BY a1.sec_id


--------------------------------------------------------------------------------
INSERT INTO securities VALUES(1, 'CK Hutchison Holdings Ltd', 'HKD', 'HK', '0001.HK');
INSERT INTO securities VALUES(2, 'CLP Holdings Ltd', 'HKD', 'HK', '0002.HK');
INSERT INTO securities VALUES(2825, 'W.I.S.E. - CSI HK 100 Tracker', 'HKD', 'HK', '2825.HK');

--------------------------------------------------------------------------------
check missing stock price by comparing sec_return.Date with sec_price.Date
For Each stock:
    If max(sec_price.Date) < max(sec_return.Date), Then
        Calculate sec_price from max(sec_price.Date) to max(sec_return.Date)

        If [sec_price on current max(sec_price.Date)] <> [derived_sec_price], Then
           Calculate sec_price from beginning to max(sec_return.Date)

--------------------------------------------------------------------------------
SELECT a1.sec_id, a1.id_yahoo, 
	IFNULL((SELECT MAX(a2.Date) FROM sec_return a2 WHERE a2.sec_id = a1.sec_id), '1900-01-01') AS [last_return_date],
	IFNULL((SELECT MAX(a2.Date) FROM sec_price a2 WHERE a2.sec_id = a1.sec_id), '1900-01-01') AS [last_price_date],
	IFNULL((SELECT a2.close FROM sec_price a2 
		WHERE a2.sec_id = a1.sec_id 
		AND a2.Date = (SELECT MAX(a3.Date) FROM sec_price a3 
			WHERE a3.sec_id = a1.sec_id)), 0) AS [last_close]
FROM securities a1
WHERE EXISTS(SELECT 1 FROM last_price a2
	WHERE a2.sec_id = a1.sec_id)

--------------------------------------------------------------------------------
-- 86400 = 60 sec * 60 min * 24 hours = 1 day
SELECT unixepoch('1900-01-01')/86400;

SELECT DATE(Date*86400, 'unixepoch') FROM sec_return WHERE sec_id = 1;

SELECT DATE(Date*86400, 'unixepoch') AS [Date], * FROM sec_price 
GROUP BY sec_id
HAVING Date = MAX(Date)

SELECT DATE(Date*86400, 'unixepoch') AS [Date], * FROM sec_price
WHERE DATE(Date*86400, 'unixepoch') = '2008-05-16'

--------------------------------------------------------------------------------
# Import Raw Data
# All
# Calculate Raw Data Return