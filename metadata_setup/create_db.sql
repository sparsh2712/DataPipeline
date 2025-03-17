create database datadump;

create schema if not exists nse;

create table nse.announcements (
    symbol varchar(255),
    description text,
    attachment_file text,
    company_name varchar(255),
    isin varchar(255),
    date timestamp,
    industry varchar(255),
    attachment_text text
);

create table nse.annxbrl (
    symbol varchar(255),
    company_name varchar(255),
    attachment_file text,
    submission_type varchar(255),
    date timestamp,
    event_type varchar(255)
);

create table nse.annualreports (
    company_name varchar(255),
    from_year int,
    to_year int,
    attachment_file text,
    symbol varchar(255)
);

create table nse.metadata(
    company_name varchar(255),
    symbol varchar(255),
    cg_record_id varchar(255),
    submission_date timestamp
);

create create schema if not exists trendlyne;

create table trendlyne.conference_calls(
    quarter varchar(255),
    financial_year varchar(255),
    playlist_id varchar(255),
    video_id varchar(255),
    video_title text
);

