drop table if exists TOMORROW_IO;
drop table if exists OPEN_WEATHER;

create table TOMORROW_IO (
id integer primary key,
date date,
location varchar(30),
temperature float,
apparent_temp float,
pressure float,
humidity float,
wind_dir float,
wind_gust float,
wind_speed float,
visibility float,
precipitation float,
weather_code integer,
rain_intensity float,
uv_value integer
);

create table OPEN_WEATHER (
id integer primary key,
date date,
location varchar(30),
temperature float,
temperature_min float,
temperature_max float,
apparent_temp float,
pressure float,
humidity float,
wind_dir float,
wind_gust float,
wind_speed float,
visibility float,
precipitation float,
weather_status varchar(40),
sunset_date date,
sunrise_date date
);
