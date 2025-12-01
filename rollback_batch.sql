BEGIN;

-- Лог-таблицы (на случай, если запускается отдельно от replace)
CREATE TABLE IF NOT EXISTS app.uuid_replace_parent_log (
    log_id      bigserial PRIMARY KEY,
    batch_id    uuid NOT NULL,
    entity_id   uuid NOT NULL,
    old_uuid    uuid NOT NULL,
    new_uuid    uuid NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app.uuid_replace_child_log (
    log_id          bigserial PRIMARY KEY,
    batch_id        uuid NOT NULL,
    child_id        bigint NOT NULL,
    old_parent_uuid uuid NOT NULL,
    new_parent_uuid uuid NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

WITH
-- 1. Общая статистика до отката
parent_total_before AS (
    SELECT COUNT(*) AS cnt FROM app.parent_entity
),
child_total_before AS (
    SELECT COUNT(*) AS cnt FROM app.child_entity
),

-- 2. Логи первой таблицы по этому batch_id (ограничены лимитом)
rollback_parent_logs AS (
    SELECT l.*
    FROM app.uuid_replace_parent_log AS l
    WHERE l.batch_id = :'batch_id'::uuid
    ORDER BY l.log_id
    LIMIT GREATEST(:'rollback_limit'::bigint, 0)
),

-- 3. Пары old/new для отката в parent
parent_pairs AS (
    SELECT DISTINCT old_uuid, new_uuid
    FROM rollback_parent_logs
),

-- 4. Пары для child по тем же uuid и тому же batch_id
child_pairs AS (
    SELECT DISTINCT cl.old_parent_uuid, cl.new_parent_uuid
    FROM app.uuid_replace_child_log AS cl
    JOIN parent_pairs AS pp
      ON cl.old_parent_uuid = pp.old_uuid
    WHERE cl.batch_id = :'batch_id'::uuid
),

-- 5. Откат второй таблицы (child): new → old
reverted_child AS (
    UPDATE app.child_entity AS c
    SET parent_id = cp.old_parent_uuid
    FROM child_pairs AS cp
    WHERE c.parent_id = cp.new_parent_uuid
    RETURNING c.id AS child_id
),

-- 6. Откат первой таблицы (parent): new → old
reverted_parent AS (
    UPDATE app.parent_entity AS p
    SET id = pp.old_uuid
    FROM parent_pairs AS pp
    WHERE p.id = pp.new_uuid
    RETURNING p.id AS entity_id
)

-- 7. Статистика отката по batch_id
SELECT
    -- Первая таблица
    (SELECT cnt FROM parent_total_before)        AS parent_total_before,
    (SELECT COUNT(*) FROM rollback_parent_logs)  AS parent_planned_rollback,
    (SELECT COUNT(*) FROM reverted_parent)       AS parent_reverted,
    (SELECT COUNT(*) FROM app.parent_entity)     AS parent_total_after,

    -- Вторая таблица
    (SELECT cnt FROM child_total_before)         AS child_total_before,
    (SELECT COUNT(*) FROM child_pairs)           AS child_planned_rollback,
    (SELECT COUNT(*) FROM reverted_child)        AS child_reverted,
    (SELECT COUNT(*) FROM app.child_entity)      AS child_total_after;

COMMIT;
