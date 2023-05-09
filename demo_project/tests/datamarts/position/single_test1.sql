select
count(1)
from {{ ref('positions' )}}
where id = '1_1'