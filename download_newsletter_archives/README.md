# Newsletter Archive Downloader

This code is used for generating a dataset for user interest prediction.

1. Create the database and tables (this was specifically created for Postgres):
```SQL
create database chromatic;
\c chromatic
create table newsletter_archives (
    nlaid serial primary key
    , created_at timestamp without time zone not null default now()
    , modified_at timestamp without time zone not null default now()
    , url text
    , full_html text
    , status integer
    , name text
);
create table newsletters (
    nlid serial primary key
    , nlaid integer references newsletter_archives
    , created_at timestamp without time zone not null default now()
    , modified_at timestamp without time zone not null default now()
    , url text
    , discovery_url text
    , full_html text
    , status integer
);
create table articles (
     aid serial primary key
     , created_at timestamp without time zone not null default now()
     , modified_at timestamp without time zone not null default now()
     , discovery_url text
     , url text
     , full_text text
     , full_html text
     , title text
     , nlid integer references newsletters
     , status integer
     , content_matches_nl_topic boolean default true
);
```
2. Copy `newsletter_archive_urls.txt.example` to `newsletter_archive_urls.txt` and provide at least one url to a newsletter archive
3. Run the `download_newsletter_archives.py` program
