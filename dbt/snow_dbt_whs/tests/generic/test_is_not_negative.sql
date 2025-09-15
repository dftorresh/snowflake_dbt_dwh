{% test is_not_negative(model, column_name) %}

with validation as (

    select
        {{ column_name }} as numberr

    from {{ model }}

),

validation_errors as (

    select
        numberr

    from validation
    where numberr < 0

)

select *
from validation_errors

{% endtest %}