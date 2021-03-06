CREATE TABLE NUMBERS (NUMBER BIGINT, X INTEGER, Y INTEGER, Z INTEGER) WITH (KAFKA_TOPIC='_NUMBERS', VALUE_FORMAT='JSON', KEY='NUMBER');

CREATE STREAM EVENTS WITH (KAFKA_TOPIC='_EVENTS', VALUE_FORMAT='AVRO');

CREATE STREAM EVENTS_ENRICHED AS SELECT NAME, MOTION->X AS X, MOTION->Y AS Y, MOTION->Z AS Z, 3 AS NUMBER FROM EVENTS;

CREATE TABLE SELECTED_WINNERS AS SELECT E.NAME AS NAME, COUNT(*) AS TOTAL FROM EVENTS_ENRICHED E LEFT OUTER JOIN NUMBERS N ON E.NUMBER = N.NUMBER WHERE E.X = N.X AND E.Y = N.Y AND E.Z = N.Z GROUP BY NAME;