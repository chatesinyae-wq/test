BEGIN;

-- Лог-таблицы (если запускается отдельно)
CREATE TABLE IF NOT EXISTS triage.uuid_replace_control_object_log (
    log_id          bigserial PRIMARY KEY,
    batch_id        uuid NOT NULL,
    parent_uuid     uuid NOT NULL,
    old_preset_uuid uuid NOT NULL,
    new_preset_uuid uuid NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS triage.uuid_replace_repository_control_object_log (
    log_id          bigserial PRIMARY KEY,
    batch_id        uuid NOT NULL,
    child_uuid      uuid NOT NULL,
    parent_uuid     uuid NOT NULL,
    old_preset_uuid uuid NOT NULL,
    new_preset_uuid uuid NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

WITH
-- 1. Общая статистика до отката
parent_total_before AS (
    SELECT COUNT(*) AS cnt FROM triage.control_object
),
child_total_before AS (
    SELECT COUNT(*) AS cnt FROM triage.repository_control_object
),

-- 2. Логи по родителям для batch_id (ограничены лимитом)
rollback_parent_logs AS (
    SELECT l.*
    FROM triage.uuid_replace_control_object_log AS l
    WHERE l.batch_id = :'batch_id'::uuid
    ORDER BY l.log_id
    LIMIT GREATEST(:'rollback_limit'::bigint, 0)
),

-- 3. Уникальные пары по родителям
parent_pairs AS (
    SELECT DISTINCT parent_uuid, old_preset_uuid, new_preset_uuid
    FROM rollback_parent_logs
),

-- 4. Уникальные записи по детям по тем же parent_uuid и batch_id
child_pairs AS (
    SELECT DISTINCT
        cl.child_uuid,
        cl.parent_uuid,
        cl.old_preset_uuid,
        cl.new_preset_uuid
    FROM triage.uuid_replace_repository_control_object_log AS cl
    JOIN parent_pairs AS pp
      ON cl.parent_uuid = pp.parent_uuid
    WHERE cl.batch_id = :'batch_id'::uuid
),

-- 5. Откат preset_uuid у детей: new → old
reverted_child AS (
    UPDATE triage.repository_control_object AS c
    SET preset_uuid = cp.old_preset_uuid
    FROM child_pairs AS cp
    WHERE c.uuid = cp.child_uuid
      AND c.preset_uuid = cp.new_preset_uuid
    RETURNING c.uuid AS child_uuid
),

-- 6. Откат preset_uuid у родителей: new → old
reverted_parent AS (
    UPDATE triage.control_object AS p
    SET preset_uuid = pp.old_preset_uuid
    FROM parent_pairs AS pp
    WHERE p.uuid = pp.parent_uuid
      AND p.preset_uuid = pp.new_preset_uuid
    RETURNING p.uuid AS parent_uuid
)

-- 7. Статистика отката по batch_id
SELECT
    -- Родители
    (SELECT cnt FROM parent_total_before)        AS parent_total_before,
    (SELECT COUNT(*) FROM rollback_parent_logs)  AS parent_planned_rollback,
    (SELECT COUNT(*) FROM reverted_parent)       AS parent_reverted,
    (SELECT COUNT(*) FROM triage.control_object) AS parent_total_after,

    -- Дети
    (SELECT cnt FROM child_total_before)         AS child_total_before,
    (SELECT COUNT(*) FROM child_pairs)           AS child_planned_rollback,
    (SELECT COUNT(*) FROM reverted_child)        AS child_reverted,
    (SELECT COUNT(*) FROM triage.repository_control_object) AS child_total_after;

COMMIT;
