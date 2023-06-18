SELECT count(*)
FROM {{ ref('positions' ) }}
WHERE id = '1_1'
