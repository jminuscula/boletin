--
-- BOE DB Summary
--
create table es_diario_boe_summary (
    summary_id varchar(14) primary key,
    pubdate date not null,
    metadata jsonb
);

create index es_diario_boe_summary_pubdate_idx on es_diario_boe_summary(pubdate);


--
-- BOE DB Article
--
create table es_diario_boe_article (
    article_id varchar(16) primary key,
    summary_id varchar(14) references es_diario_boe_summary(summary_id),
    pubdate date not null,
    metadata jsonb,
    title text,
    title_search tsvector generated always as (to_tsvector('spanish', title)) stored,
    title_summary text,
    -- text-embedding-ada-002
    title_embedding vector(1536)
);

create index es_diario_boe_article_pubdate_idx on es_diario_boe_article(pubdate);
create index es_diario_boe_title_search_idx on es_diario_boe_article using gin(title_search);


create table es_diario_boe_article_fragment (
    article_id varchar(16) references es_diario_boe_article(article_id),
    sequence smallint,
    content text,
    content_search tsvector generated always as (to_tsvector('spanish', content)) stored,
    summary text,
    -- text-embedding-ada-002
    embedding vector(1536),
    primary key (article_id, sequence)
);

create index es_diario_boe_content_search_idx on es_diario_boe_article_fragment using gin(content_search);


--
-- BOE DB Search lexemes
--
create materialized view es_diario_boe_article_lexemes as
select
    distinct word as lexeme
from
    (
        select
            word
        from
            ts_stat(
                'select title_search from es_diario_boe_article'
            )
        union
        select
            word
        from
            ts_stat(
                'select content_search from es_diario_boe_article_fragment'
            )
    );

create index es_diario_boe_article_lexemes_idx on es_diario_boe_article_lexemes(lexeme);