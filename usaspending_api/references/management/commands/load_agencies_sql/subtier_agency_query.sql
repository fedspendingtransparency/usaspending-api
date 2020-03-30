select
    subtier_code,
    max(subtier_abbreviation) as abbreviation,
    max(subtier_name) as name
from
    "{temp_table}"
where
    subtier_code is not null and
    subtier_name is not null
group by
    subtier_code
