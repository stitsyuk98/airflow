CREATE TABLE IF NOT EXISTS public.cell_towers
(
    radio String,
    mcc Int64,
    net Int64,
    area Int64,
    cell Int64,
    unit Int64,
    lon Float64,
    lat Float64,
    range Int64,
    samples Int64,
    changeable Int64,
    created String,
    updated String,
    averageSignal Int64,
    hash_id String
) 
ENGINE = ReplacingMergeTree
ORDER BY hash_id
