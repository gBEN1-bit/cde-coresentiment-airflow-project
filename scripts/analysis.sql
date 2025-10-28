---NOTE:
-- Replace the timestamp with the hour you processed
-- Example: '2025-10-10 04:00:00'
-- Replace the company names with the list you want to filter
SELECT 
    page_title, 
    SUM(view_count) AS total_views
FROM 
    public.wikipedia_pageviews
WHERE 
    hour_timestamp = '2025-10-10 04:00:00'
    AND page_title IN ('Amazon', 'Apple', 'Facebook', 'Google', 'Microsoft')
GROUP BY 
    page_title
ORDER BY 
    total_views DESC;


#If you want to highlight the company with the highest views in the results:
SELECT 
    page_title, 
    SUM(view_count) AS total_views
FROM 
    public.wikipedia_pageviews
WHERE 
    hour_timestamp = '2025-10-10 04:00:00'
    AND page_title IN ('Amazon', 'Apple', 'Facebook', 'Google', 'Microsoft')
GROUP BY 
    page_title
ORDER BY 
    total_views DESC
LIMIT 1;

