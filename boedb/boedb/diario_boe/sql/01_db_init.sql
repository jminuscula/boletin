CREATE USER boedb WITH PASSWORD 'boedb';
CREATE SCHEMA boedb;
GRANT ALL ON SCHEMA boedb TO boedb;

CREATE DATABASE boedb OWNER boedb;

\c boedb boedb;
CREATE EXTENSION vector;