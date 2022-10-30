


WITH T(domain, reported_resource_count) AS (
    select E.value->>'$.domain' as domain,
        E.value->>'$.count' as reported_resource_count
    FROM  JSON_EACH(readfile('socrata_domains.json'), '$.results') as E
)
insert into domain(domain, resource_count)
select domain,
    reported_resource_count as resource_count
from T
WHERE true ON CONFLICT(domain) DO -- https://www.sqlite.org/lang_upsert.html
UPDATE
SET resource_count = excluded.resource_count
WHERE COALESCE(resource_count, '') != COALESCE(excluded.resource_count,'');


INSERT INTO
    socrata_domain_of_interest(domain, notes)
VALUES
    ('data.cityofnewyork.us', 'tgrid demo'),
    ('datahub.transportation.gov',NULL),
    ('data.energystar.gov', NULL),
    ('data.cambridgema.gov', NULL)
ON CONFLICT DO NOTHING;



