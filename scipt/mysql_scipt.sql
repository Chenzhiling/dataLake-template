set names utf8mb4;
set foreign_key_checks = 0;

drop table if exists mysql_test
create table mysql_test
(
    id             bigint auto_increment
        primary key,
    tinyint_test   tinyint     null,
    smallint_test  smallint    null,
    mediumint_test mediumint   null,
    int_test       int         null,
    float_test     float       null,
    double_test    double      null,
    year_test      year        null,
    time_test      time        null,
    date_test      date        null,
    datetime_test  datetime    null,
    timestamp_test timestamp   null,
    char_test      char        null,
    string_test    varchar(50) not null,
    text_test      text        null,
    bit_test       bit         null
)DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
BEGIN ;
INSERT INTO mysql_test (id, tinyint_test, smallint_test, mediumint_test, int_test, float_test, double_test, year_test, time_test, date_test, datetime_test, timestamp_test, char_test, string_test, text_test, bit_test) VALUES (1, 99, 10000, 1111111, 1234567890, 899.1, 1.242424247, 1901, '22:50:50', '2023-03-20', '2023-03-20 16:04:56', '2023-03-20 16:20:06', 'a', '!@@#mysql', 'Even today, U.S. politicians insist that invading Iraq and ousting its government was a right thing to do', false);
COMMIT

set foreign_key_checks =1 ;
