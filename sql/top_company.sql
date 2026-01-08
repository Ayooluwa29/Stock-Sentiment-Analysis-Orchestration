SELECT 
    company_name,
    view_count,
    data_date,
    data_hour
FROM company_pageviews
ORDER BY view_count DESC
LIMIT 1;