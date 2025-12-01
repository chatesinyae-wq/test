BEGIN;

-- Лог-таблицы (создаются один раз, если нет)

CREATE TABLE IF NOT EXISTS triage.uuid_replace_control_object_log (
    log_id          bigserial PRIMARY KEY,
    batch_id        uuid NOT NULL,          -- идентификатор запуска
    parent_uuid     uuid NOT NULL,          -- triage.control_object.uuid
    old_preset_uuid uuid NOT NULL,          -- preset_uuid до
    new_preset_uuid uuid NOT NULL,          -- preset_uuid после
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS triage.uuid_replace_repository_control_object_log (
    log_id          bigserial PRIMARY KEY,
    batch_id        uuid NOT NULL,
    child_uuid      uuid NOT NULL,          -- triage.repository_control_object.uuid
    parent_uuid     uuid NOT NULL,          -- triage.repository_control_object.parent_uuid
    old_preset_uuid uuid NOT NULL,          -- preset_uuid до
    new_preset_uuid uuid NOT NULL,          -- preset_uuid после
    created_at      timestamptz NOT NULL DEFAULT now()
);

WITH
-- 1. Родители-кандидаты: у кого preset_uuid = old_uuid
candidate_parents AS (
    SELECT p.uuid AS parent_uuid
    FROM triage.control_object AS p
    WHERE p.preset_uuid = :'old_uuid'::uuid
    ORDER BY p.uuid
),

-- 2. Ограничиваемся лимитом по родителям
selected_parents AS (
    SELECT cp.parent_uuid
    FROM candidate_parents AS cp
    LIMIT GREATEST(:'parent_limit'::bigint, 0)
),

-- 3. Общая статистика до замены
parent_total_before AS (
    SELECT COUNT(*) AS cnt FROM triage.control_object
),
child_total_before AS (
    SELECT COUNT(*) AS cnt FROM triage.repository_control_object
),

-- 4. Сколько реально подходит под замену
parent_target_before AS (
    SELECT COUNT(*) AS cnt
    FROM triage.control_object AS p
    JOIN selected_parents AS sp ON p.uuid = sp.parent_uuid
),
child_target_before AS (
    SELECT COUNT(*) AS cnt
    FROM triage.repository_control_object AS c
    JOIN selected_parents AS sp ON c.parent_uuid = sp.parent_uuid
    WHERE c.preset_uuid = :'old_uuid'::uuid
),

-- 5. Логируем изменения родителей
logged_parent AS (
    INSERT INTO triage.uuid_replace_control_object_log (
        batch_id, parent_uuid, old_preset_uuid, new_preset_uuid
    )
    SELECT
        :'batch_id'::uuid,
        p.uuid,
        :'old_uuid'::uuid,
        :'new_uuid'::uuid
    FROM triage.control_object AS p
    JOIN selected_parents AS sp ON p.uuid = sp.parent_uuid
    RETURNING parent_uuid
),

-- 6. Обновляем preset_uuid у родителей
updated_parent AS (
    UPDATE triage.control_object AS p
    SET preset_uuid = :'new_uuid'::uuid
    FROM selected_parents AS sp
    WHERE p.uuid = sp.parent_uuid
      AND p.preset_uuid = :'old_uuid'::uuid
    RETURNING p.uuid AS parent_uuid
),

-- 7. Логируем изменения детей
logged_child AS (
    INSERT INTO triage.uuid_replace_repository_control_object_log (
        batch_id, child_uuid, parent_uuid, old_preset_uuid, new_preset_uuid
    )
    SELECT
        :'batch_id'::uuid,
        c.uuid,
        c.parent_uuid,
        :'old_uuid'::uuid,
        :'new_uuid'::uuid
    FROM triage.repository_control_object AS c
    JOIN selected_parents AS sp ON c.parent_uuid = sp.parent_uuid
    WHERE c.preset_uuid = :'old_uuid'::uuid
    RETURNING child_uuid
),

-- 8. Обновляем preset_uuid у детей
updated_child AS (
    UPDATE triage.repository_control_object AS c
    SET preset_uuid = :'new_uuid'::uuid
    FROM selected_parents AS sp
    WHERE c.parent_uuid = sp.parent_uuid
      AND c.preset_uuid = :'old_uuid'::uuid
    RETURNING c.uuid AS child_uuid
)

-- 9. Простая статистика "было / заменили / стало"
SELECT
    -- Родители
    (SELECT cnt FROM parent_total_before)                                         AS parent_total_before,
    (SELECT cnt FROM parent_target_before)                                        AS parent_target_before,
    (SELECT COUNT(*) FROM updated_parent)                                         AS parent_replaced,
    (SELECT COUNT(*) FROM triage.control_object AS p
      JOIN selected_parents AS sp ON p.uuid = sp.parent_uuid
      WHERE p.preset_uuid = :'new_uuid'::uuid)                                    AS parent_target_after,

    -- Дети
    (SELECT cnt FROM child_total_before)                                          AS child_total_before,
    (SELECT cnt FROM child_target_before)                                         AS child_target_before,
    (SELECT COUNT(*) FROM updated_child)                                          AS child_replaced,
    (SELECT COUNT(*) FROM triage.repository_control_object AS c
      JOIN selected_parents AS sp ON c.parent_uuid = sp.parent_uuid
      WHERE c.preset_uuid = :'new_uuid'::uuid)                                    AS child_target_after;

COMMIT;
