-- scripts/init_db.sql
CREATE TABLE IF NOT EXISTS postgres.public.wikipedia_pageviews (
    id serial PRIMARY KEY,
    domain VARCHAR,
    page_title VARCHAR,
    view_count INTEGER,
    response_size INTEGER,
    hour_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT now()
);
-- optionally create an index for quick lookups
CREATE INDEX IF NOT EXISTS idx_wpv_page_title ON postgres.public.wikipedia_pageviews (page_title);
CREATE INDEX IF NOT EXISTS idx_wpv_hour_timestamp ON postgres.public.wikipedia_pageviews (hour_timestamp);
