--
-- BOE DB Summary
--
create table es_diario_boe_summary (
    summary_id varchar(14) primary key,
    pubdate date not null,
    metadata jsonb,
    n_articles smallint
);

create index es_diario_boe_summary_pubdate_idx on es_diario_boe_summary(pubdate);


--
-- BOE DB Article
--
create table es_diario_boe_article (
    article_id varchar(16) primary key,
    summary_id varchar(14) references es_diario_boe_summary(summary_id) on delete cascade,
    pubdate date not null,
    metadata jsonb,
    title text,
    title_search tsvector generated always as (to_tsvector('spanish', title)) stored,
    title_summary text,
    -- text-embedding-ada-002
    title_embedding vector(1536),
    n_fragments smallint
);

create index es_diario_boe_article_pubdate_idx on es_diario_boe_article(pubdate);
create index es_diario_boe_title_search_idx on es_diario_boe_article using gin(title_search);


create table es_diario_boe_article_fragment (
    article_id varchar(16) references es_diario_boe_article(article_id) on delete cascade,
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


--
-- BOE DB Integrity
--
create view es_diario_boe_summary_incomplete as
select
    sum.summary_id,
    sum.n_articles,
    count(art.article_id) as n_articles_available
from
    es_diario_boe_summary sum
    left join es_diario_boe_article art on sum.summary_id = art.summary_id
group by
    sum.summary_id
having
    count(art.article_id) < sum.n_articles;

create view es_diario_boe_article_incomplete as
select
    art.article_id,
    art.n_fragments,
    count(frag.article_id) as n_fragments_available
from
    es_diario_boe_article art
    left join es_diario_boe_article_fragment frag on art.article_id = frag.article_id
group by
    art.article_id
having
    count(frag.article_id) < art.n_fragments;

create procedure es_diario_boe_delete_incomplete_articles()
begin atomic
    delete from es_diario_boe_article where article_id in (
        select article_id from es_diario_boe_article_incomplete
    );
end;