CREATE DATABASE proyecto02;

CREATE USER 'pr'@'%' IDENTIFIED BY 'pr';

GRANT SUPER ON *.* TO 'pr'@'%';
GRANT ALL PRIVILEGES ON proyecto02.* TO 'pr'@'%';
FLUSH PRIVILEGES;

SET GLOBAL log_bin_trust_function_creators = 1;
