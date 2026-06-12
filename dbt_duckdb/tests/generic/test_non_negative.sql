-- tests/generic/test_non_negative.sql
-- Usage in schema.yml:
--   tests:
--     - non_negative

{% test non_negative(model, column_name) %}

select
    {{ column_name }},
    count(*) as failing_rows
from {{ model }}
where {{ column_name }} < 0
group by 1

{% endtest %}
