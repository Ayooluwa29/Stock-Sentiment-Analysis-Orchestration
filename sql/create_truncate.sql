-- Create table if not exists
CREATE TABLE IF NOT EXISTS company_pageviews (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(500),
    view_count INTEGER,
    data_date DATE,
    data_hour TIME,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CREATE INDEX IF NOT EXISTS idx_view_count ON company_pageviews(view_count DESC);

-- Truncate table
TRUNCATE TABLE company_pageviews;