pipeline {
    agent any

    options {
        timestamps()
        ansiColor('xterm')
        timeout(time: 30, unit: 'MINUTES')
    }

    // Подстрой под свой стенд
    environment {
        DB_HOST = 'postgres'   // или имя/адрес БД
        DB_PORT = '5432'
        DB_NAME = 'mydb'
    }

    parameters {
        choice(
            name: 'MODE',
            choices: ['REPLACE', 'ROLLBACK_BATCH', 'ROLLBACK_DATE'],
            description: 'Режим работы: REPLACE — замена UUID, ROLLBACK_BATCH — откат по batch_id, ROLLBACK_DATE — откат по дате'
        )

        // Для REPLACE
        string(
            name: 'OLD_UUID',
            defaultValue: '',
            description: 'Старый UUID (для режима REPLACE)'
        )
        string(
            name: 'NEW_UUID',
            defaultValue: '',
            description: 'Новый UUID (для режима REPLACE)'
        )
        string(
            name: 'PARENT_LIMIT',
            defaultValue: '1',
            description: 'Максимум строк первой таблицы для замены (REPLACE)'
        )

        // Для отката по batch_id
        string(
            name: 'BATCH_ID',
            defaultValue: '',
            description: 'batch_id для отката в режиме ROLLBACK_BATCH; для REPLACE, если пусто, будет сгенерирован новый'
        )
        string(
            name: 'ROLLBACK_LIMIT',
            defaultValue: '100',
            description: 'Максимум строк первой таблицы для отката (ROLLBACK_BATCH / ROLLBACK_DATE)'
        )

        // Для отката по дате
        string(
            name: 'ROLLBACK_FROM',
            defaultValue: '',
            description: 'Начало интервала отката (timestamptz, напр. 2025-12-01T00:00:00+03:00)'
        )
        string(
            name: 'ROLLBACK_TO',
            defaultValue: '',
            description: 'Конец интервала отката (timestamptz, не включительно, напр. 2025-12-02T00:00:00+03:00)'
        )
    }

    stages {
        stage('Print parameters') {
            steps {
                script {
                    echo "MODE            = ${params.MODE}"
                    echo "OLD_UUID        = ${params.OLD_UUID}"
                    echo "NEW_UUID        = ${params.NEW_UUID}"
                    echo "PARENT_LIMIT    = ${params.PARENT_LIMIT}"
                    echo "BATCH_ID        = ${params.BATCH_ID}"
                    echo "ROLLBACK_LIMIT  = ${params.ROLLBACK_LIMIT}"
                    echo "ROLLBACK_FROM   = ${params.ROLLBACK_FROM}"
                    echo "ROLLBACK_TO     = ${params.ROLLBACK_TO}"
                }
            }
        }

        stage('Run SQL via psql') {
            steps {
                script {
                    // Лимиты как строки, psql сам приведет к числам
                    def parentLimit   = params.PARENT_LIMIT?.trim()    ?: '0'
                    def rollbackLimit = params.ROLLBACK_LIMIT?.trim() ?: '0'

                    // Для REPLACE: либо берём переданный BATCH_ID, либо генерируем новый
                    def effectiveBatchId = params.BATCH_ID?.trim()
                    if (!effectiveBatchId) {
                        effectiveBatchId = java.util.UUID.randomUUID().toString()
                    }

                    echo "Effective BATCH_ID (для этого запуска REPLACE/ROLLBACK_BATCH) = ${effectiveBatchId}"

                    // Используем креды Jenkins для подключения к БД
                    // Создай в Jenkins credentials с ID 'psql-app-user' (username + password)
                    withCredentials([usernamePassword(
                        credentialsId: 'psql-app-user',
                        usernameVariable: 'DB_USER',
                        passwordVariable: 'DB_PASSWORD'
                    )]) {

                        // Общая заготовка psql-команды (без -v и -f)
                        def basePsql = """
PGPASSWORD="$DB_PASSWORD" psql \\
  -h "$DB_HOST" \\
  -p "$DB_PORT" \\
  -U "$DB_USER" \\
  -d "$DB_NAME" \\
  -v ON_ERROR_STOP=1
""".stripIndent().trim()

                        // Хелпер, чтобы не дублировать вызов sh
                        def runPsql = { String extraVars, String sqlFile ->
                            sh """
                                ${basePsql} \\
                                  ${extraVars} \\
                                  -f "${sqlFile}"
                            """
                        }

                        if (params.MODE == 'REPLACE') {
                            if (!params.OLD_UUID?.trim() || !params.NEW_UUID?.trim()) {
                                error "Для MODE=REPLACE нужно задать OLD_UUID и NEW_UUID"
                            }

                            echo "Запуск режима REPLACE (замена UUID)..."

                            // В репо должен лежать файл sql/replace_uuid.sql
                            // внутри которого используются psql-переменные:
                            //  :'batch_id'::uuid
                            //  :'old_uuid'::uuid
                            //  :'new_uuid'::uuid
                            //  :'parent_limit'::bigint  (через GREATEST и т.п.)
                            runPsql(
                                "-v batch_id=${effectiveBatchId} " +
                                "-v old_uuid=${params.OLD_UUID.trim()} " +
                                "-v new_uuid=${params.NEW_UUID.trim()} " +
                                "-v parent_limit=${parentLimit}",
                                "sql/replace_uuid.sql"
                            )

                        } else if (params.MODE == 'ROLLBACK_BATCH') {
                            if (!params.BATCH_ID?.trim()) {
                                error "Для MODE=ROLLBACK_BATCH нужно задать BATCH_ID"
                            }

                            echo "Запуск режима ROLLBACK_BATCH (откат по batch_id)..."

                            // В репо должен лежать файл sql/rollback_batch.sql
                            // Внутри используются переменные:
                            //  :'batch_id'::uuid
                            //  :'rollback_limit'::bigint
                            runPsql(
                                "-v batch_id=${params.BATCH_ID.trim()} " +
                                "-v rollback_limit=${rollbackLimit}",
                                "sql/rollback_batch.sql"
                            )

                        } else if (params.MODE == 'ROLLBACK_DATE') {
                            if (!params.ROLLBACK_FROM?.trim() || !params.ROLLBACK_TO?.trim()) {
                                error "Для MODE=ROLLBACK_DATE нужно задать ROLLBACK_FROM и ROLLBACK_TO (формат timestamptz)"
                            }

                            echo "Запуск режима ROLLBACK_DATE (откат по интервалу дат)..."

                            // В репо должен лежать файл sql/rollback_date.sql
                            // Внутри используются переменные:
                            //  :'rollback_from'::timestamptz
                            //  :'rollback_to'::timestamptz
                            //  :'rollback_limit'::bigint
                            runPsql(
                                "-v rollback_from=${params.ROLLBACK_FROM.trim()} " +
                                "-v rollback_to=${params.ROLLBACK_TO.trim()} " +
                                "-v rollback_limit=${rollbackLimit}",
                                "sql/rollback_date.sql"
                            )

                        } else {
                            error "Неизвестный MODE=${params.MODE}"
                        }
                    }
                }
            }
        }
    }
}
