WITH count_cell as (
    SELECT area, count(*) as count_cell
    FROM public.cell_towers
    GROUP BY area
    HAVING count_cell > 2000
)
SELECT area 
FROM public.cell_towers
WHERE mcc = 250 AND radio != 'LTE' AND area IN (
    SELECT area
    FROM count_cell
)