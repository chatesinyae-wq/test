-- Первая таблица (по ней считаем лимиты)
-- PK = uuid
-- app.parent_entity(id uuid primary key, ...)

-- Вторая таблица (отношение 1:N)
-- app.child_entity(id bigint primary key, parent_id uuid not null, ...)

BEGIN;

-- Лог-таблицы (создаются один раз, если не были созданы)
CREATE TABLE IF NOT EXISTS app.uuid_replace_parent_log (
    log_id      bigserial PRIMARY KEY,
    batch_id    uuid NOT NULL,          -- идентификатор запуска
    entity_id   uuid NOT NULL,          -- PK первой таблицы до замены
    old_uuid    uuid NOT NULL,          -- старое значение
    new_uuid    uuid NOT NULL,          -- новое значение
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app.uuid_replace_child_log (
    log_id          bigserial PRIMARY KEY,
    batch_id        uuid NOT NULL,
    child_id        bigint NOT NULL,    -- PK второй таблицы
    old_parent_uuid uuid NOT NULL,      -- parent_id до
    new_parent_uuid uuid NOT NULL,      -- parent_id после
    created_at      timestamptz NOT NULL DEFAULT now()
);

WITH
-- 1. Кандидаты в первой таблице под старый uuid
candidate_parents AS (
    SELECT p.id AS old_uuid
    FROM app.parent_entity AS p
    WHERE p.id = :'old_uuid'::uuid
    ORDER BY p.id
),

-- 2. Ограничиваемся лимитом по первой таблице
selected_parents AS (
    SELECT cp.old_uuid
    FROM candidate_parents AS cp
    LIMIT GREATEST(:'parent_limit'::bigint, 0)
),

-- 3. Общая статистика до замены
parent_total_before AS (
    SELECT COUNT(*) AS cnt FROM app.parent_entity
),
child_total_before AS (
    SELECT COUNT(*) AS cnt FROM app.child_entity
),

-- 4. Сколько реально подходит под замену
parent_target_before AS (
    SELECT COUNT(*) AS cnt
    FROM app.parent_entity AS p
    JOIN selected_parents AS sp ON p.id = sp.old_uuid
),
child_target_before AS (
    SELECT COUNT(*) AS cnt
    FROM app.child_entity AS c
    JOIN selected_parents AS sp ON c.parent_id = sp.old_uuid
),

-- 5. Логируем изменения первой таблицы
logged_parent AS (
    INSERT INTO app.uuid_replace_parent_log (batch_id, entity_id, old_uuid, new_uuid)
    SELECT
        :'batch_id'::uuid AS batch_id,
        p.id              AS entity_id,
        sp.old_uuid       AS old_uuid,
        :'new_uuid'::uuid AS new_uuid
    FROM app.parent_entity AS p
    JOIN selected_parents AS sp ON p.id = sp.old_uuid
    RETURNING entity_id
),

-- 6. Обновляем первую таблицу
updated_parent AS (
    UPDATE app.parent_entity AS p
    SET id = :'new_uuid'::uuid
    FROM selected_parents AS sp
    WHERE p.id = sp.old_uuid
    RETURNING p.id AS new_id
),

-- 7. Логируем изменения второй таблицы
logged_child AS (
    INSERT INTO app.uuid_replace_child_log (batch_id, child_id, old_parent_uuid, new_parent_uuid)
    SELECT
        :'batch_id'::uuid AS batch_id,
        c.id              AS child_id,
        sp.old_uuid       AS old_parent_uuid,
        :'new_uuid'::uuid AS new_parent_uuid
    FROM app.child_entity AS c
    JOIN selected_parents AS sp ON c.parent_id = sp.old_uuid
    RETURNING child_id
),

-- 8. Обновляем вторую таблицу
updated_child AS (
    UPDATE app.child_entity AS c
    SET parent_id = :'new_uuid'::uuid
    FROM selected_parents AS sp
    WHERE c.parent_id = sp.old_uuid
    RETURNING c.id AS child_id
)

-- 9. Простая статистика "было / заменили / стало"
SELECT
    -- Первая таблица
    (SELECT cnt FROM parent_total_before)                                       AS parent_total_before,
    (SELECT cnt FROM parent_target_before)                                      AS parent_target_before,
    (SELECT COUNT(*) FROM updated_parent)                                       AS parent_replaced,
    (SELECT COUNT(*) FROM app.parent_entity AS p
      WHERE p.id = :'new_uuid'::uuid)                                           AS parent_target_after,

    -- Вторая таблица
    (SELECT cnt FROM child_total_before)                                        AS child_total_before,
    (SELECT cnt FROM child_target_before)                                       AS child_target_before,
    (SELECT COUNT(*) FROM updated_child)                                        AS child_replaced,
    (SELECT COUNT(*) FROM app.child_entity AS c
      WHERE c.parent_id = :'new_uuid'::uuid)                                    AS child_target_after;

COMMIT;
