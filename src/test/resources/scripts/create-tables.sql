DROP TABLE IF EXISTS entries;

CREATE TABLE IF NOT EXISTS entries (
  id INTEGER AUTO_INCREMENT ,
--   id bigserial
  entryid VARCHAR UNIQUE NOT NULL,
--   entryid text UNIQUE NOT NULL,
  creationdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--   creationdate timestamp without time zone NOT NULL DEFAULT current_timestamp,
  datelastupdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--   datelastupdated timestamp without time zone NOT NULL DEFAULT current_timestamp,
  entrybody VARCHAR,
--   entrybody text
  feed VARCHAR,
--   feed text,
  categories ARRAY,
--   categories character varying[],
  eventtype VARCHAR,
--   eventtype text,
  tenantid VARCHAR,
--   tenantid text,
  PRIMARY KEY(datelastupdated, id)
);