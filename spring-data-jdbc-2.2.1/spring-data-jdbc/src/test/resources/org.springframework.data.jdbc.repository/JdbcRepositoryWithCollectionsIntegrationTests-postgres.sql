DROP TABLE element;
DROP TABLE dummy_entity;
CREATE TABLE dummy_entity ( id SERIAL PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE element (id SERIAL PRIMARY KEY, content VARCHAR(100), dummy_entity BIGINT);
