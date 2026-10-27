SELECT id, shard_key::text AS composite_value,
       pg_typeof(shard_key)::text AS composite_type,
       hash_record(shard_key) AS hash_value,
       MOD(ABS(hash_record(shard_key)), 4) AS bucket,
       shard_key IS NULL AS is_null,
       shard_key IS NOT NULL AS is_not_null,
       MOD(ABS(hash_record(shard_key)), 4) = 2 AS base_task_2,
       ((shard_key IS NULL) OR MOD(ABS(hash_record(shard_key)), 4) = 0) AS head_task_0,
       ((shard_key IS NOT NULL) AND MOD(ABS(hash_record(shard_key)), 4) = 1) AS head_task_1,
       ((shard_key IS NOT NULL) AND MOD(ABS(hash_record(shard_key)), 4) = 2) AS head_task_2,
       ((shard_key IS NOT NULL) AND MOD(ABS(hash_record(shard_key)), 4) = 3) AS head_task_3
FROM events WHERE id = 1;
