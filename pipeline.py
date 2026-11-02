import logging
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, URL
import time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            "logs/pipeline.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# =====================================
# 1. НАСТРОЙКИ ПОДКЛЮЧЕНИЯ
# =====================================

load_dotenv()


def get_engine():
    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME")
    )

    return create_engine(database_url)



def check_database_connection(max_attempts=3, delay=5):
    logger.info("Проверка подключения к PostgreSQL")

    engine = get_engine()

    try:
        for attempt in range(1, max_attempts + 1):
            try:
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))

                logger.info("PostgreSQL доступен")
                return

            except Exception as error:
                logger.warning(
                    f"Попытка {attempt}/{max_attempts} "
                    f"не удалась: {error}"
                )

                if attempt < max_attempts:
                    logger.info(
                        f"Повторное подключение через {delay} секунд"
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "Не удалось подключиться к PostgreSQL"
                    )
                    raise
    finally:
        engine.dispose()


# =====================================
# 2. EXTRACT — ЗАГРУЗКА ИСХОДНЫХ ДАННЫХ
# =====================================

def extract():
    df = pd.read_csv("data/orders.csv")

    print("Данные успешно загружены!")
    print(df)

    return df


# =====================================
# 3. TRANSFORM — ОЧИСТКА ДАННЫХ
# =====================================

def transform(df):
    df_clean = df[
        (df["amount"] > 0) &
        (df["status"] == "paid")
    ].copy()

    print("Данные после очистки:")
    print(df_clean)

    return df_clean


def data_quality_report(df):
    total_rows = len(df)

    
    technical_errors = (
        df["order_id"].isnull() |
        df["order_id"].duplicated(keep=False) |
        df["amount"].isnull()
    )

    technical_error_count = technical_errors.sum()

    quality_score = (
        (total_rows - technical_error_count) / total_rows * 100
        if total_rows > 0 else 0
    )

    logger.info(
        f"Data Quality Score: {quality_score:.2f}%"
    )


    cancelled_orders = (df["status"] == "cancelled").sum()

    invalid_amounts = (df["amount"] <= 0).sum()

    duplicate_orders = df["order_id"].duplicated().sum()

    valid_orders = (
        (df["status"] == "paid") &
        (df["amount"] > 0)
    ).sum()

    valid_percent = (
        valid_orders / total_rows * 100
        if total_rows > 0 else 0
    )
    logger.info(
        f"Доля заказов после бизнес-фильтрации: {valid_percent:.2f}%"
    )
    
    logger.info("===== DATA QUALITY REPORT =====")
    logger.info(f"Всего строк: {total_rows}")
    logger.info(f"Корректных заказов: {valid_orders}")
    logger.info(f"Отменённых заказов: {cancelled_orders}")
    logger.info(f"Некорректных сумм: {invalid_amounts}")
    logger.info(f"Дубликатов order_id: {duplicate_orders}")
    logger.info(f"Доля корректных заказов: {valid_percent:.2f}%")
    logger.info("===============================")
    
    report = pd.DataFrame([{
        "run_time": datetime.now().isoformat(timespec="seconds"),
        "total_rows": total_rows,
        "valid_orders": valid_orders,
        "cancelled_orders": cancelled_orders,
        "invalid_amounts": invalid_amounts,
        "duplicate_orders": duplicate_orders,
        "valid_percent": round(valid_percent, 2),
        "quality_score": round(quality_score, 2)
    }])

    report_path = "output/data_quality_history.csv"

    file_exists = os.path.exists(report_path)

    report.to_csv(
        report_path,
        mode="a",
        header=not file_exists,
        index=False
    )

    logger.info("Отчёт о качестве данных сохранён в CSV")


def validate_data(df):
    logger.info("Начинается проверка качества данных")

    if df.empty:
        raise ValueError("Нет данных для загрузки")

    if df["order_id"].isnull().any():
        raise ValueError("Обнаружены пустые order_id")

    if df["order_id"].duplicated().any():
        raise ValueError("Обнаружены дубликаты order_id")

    if df["amount"].isnull().any():
        raise ValueError("Обнаружены пустые суммы заказов")

    if (df["amount"] <= 0).any():
        raise ValueError("Обнаружены некорректные суммы заказов")

    logger.info("Проверка качества данных пройдена успешно")

# =====================================
# 4. LOAD — СОХРАНЕНИЕ В CSV
# =====================================

def load(df):
    df.to_csv(
        "output/orders_clean.csv",
        index=False
    )

    print("Данные успешно сохранены!")


# =====================================
# 5. LOAD — ЗАГРУЗКА В POSTGRESQL
# =====================================


def load_to_postgres(df, connection):
    query = text("""
        INSERT INTO orders (
            order_id, order_date, customer_id, amount, status, is_active
        )
        VALUES (
            :order_id, :order_date, :customer_id, :amount, :status, TRUE
        )
        ON CONFLICT (order_id)
        DO UPDATE SET
            order_date = EXCLUDED.order_date,
            customer_id = EXCLUDED.customer_id,
            amount = EXCLUDED.amount,
            status = EXCLUDED.status,
            is_active = TRUE
    """)

    records = df.to_dict(orient="records")

    if records:
        connection.execute(query, records)

    logger.info("UPSERT заказов выполнен")




def soft_delete_missing_orders(
    df,
    connection,
    full_snapshot=False
):
    logger.info("Начинается синхронизация активности заказов")

    if not full_snapshot:
        logger.info(
            "Soft Delete пропущен: "
            "полный снимок данных не подтверждён"
        )
        return

    if df.empty:
        raise ValueError(
            "Нельзя выполнять Soft Delete с пустым набором данных"
        )

    order_ids = df["order_id"].tolist()

    query = text("""
        UPDATE orders
        SET is_active = FALSE
        WHERE is_active = TRUE
          AND NOT (order_id = ANY(:order_ids))
    """)

    result = connection.execute(
        query,
        {"order_ids": order_ids}
    )

    logger.info(
        f"Помечено неактивными заказов: {result.rowcount}"
    )


# =====================================
# 6. СОЗДАНИЕ АНАЛИТИЧЕСКОЙ ВИТРИНЫ
# =====================================

def create_data_mart():
    engine = get_engine()

    
    query = text("""
    CREATE OR REPLACE VIEW daily_sales AS
    SELECT
        order_date,
        COUNT(*) AS orders_count,
        SUM(amount) AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_order_value
    FROM orders
    WHERE is_active = TRUE
    GROUP BY order_date
""")


    with engine.begin() as connection:
        connection.execute(query)

    engine.dispose()

    print("Аналитическая витрина daily_sales готова!")


# =====================================
# 7. ЗАПУСК ETL-ПАЙПЛАЙНА
# =====================================


def check_snapshot_completeness(df, connection, threshold=0.20):
    logger.info("Проверка полноты снимка данных")

    result = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM orders
            WHERE is_active = TRUE
        """)
    )

    previous_count = result.scalar_one()
    current_count = len(df)

    if previous_count == 0:
        logger.info(
            "В PostgreSQL нет активных заказов для сравнения"
        )
        return False

    decrease = (previous_count - current_count) / previous_count

    logger.info(
        f"Активных заказов в PostgreSQL: {previous_count}"
    )
    logger.info(
        f"Заказов в новом снимке: {current_count}"
    )

    if decrease > threshold:
        logger.warning(
            f"Количество заказов сократилось на "
            f"{decrease:.2%}. "
            "Требуется проверка полноты источника."
        )
        return False

    logger.info("Резкого сокращения заказов не обнаружено")
    return True


def start_etl_run():
    logger.info("Регистрация нового запуска ETL")

    engine = get_engine()

    try:
        with engine.begin() as connection:
            result = connection.execute(
                text("""
                    INSERT INTO etl_run_history (status)
                    VALUES ('running')
                    RETURNING run_id
                """)
            )

            run_id = result.scalar_one()

        logger.info(
            f"Запуск ETL зарегистрирован: run_id={run_id}"
        )

        return run_id

    finally:
        engine.dispose()


def finish_etl_run(
    run_id,
    status,
    rows_extracted=None,
    rows_loaded=None,
    error_message=None
):
    logger.info(
        f"Обновление статуса ETL: run_id={run_id}, status={status}"
    )

    engine = get_engine()

    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    UPDATE etl_run_history
                    SET
                        finished_at = NOW(),
                        status = :status,
                        rows_extracted = :rows_extracted,
                        rows_loaded = :rows_loaded,
                        error_message = :error_message
                    WHERE run_id = :run_id
                """),
                {
                    "run_id": run_id,
                    "status": status,
                    "rows_extracted": rows_extracted,
                    "rows_loaded": rows_loaded,
                    "error_message": error_message
                }
            )

        logger.info(
            f"История запуска ETL обновлена: run_id={run_id}"
        )

    finally:
        engine.dispose()


def check_stale_etl_runs(timeout_minutes=10):
    logger.info("Проверка зависших запусков ETL")

    engine = get_engine()

    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("""
                    SELECT
                        run_id,
                        started_at,
                        NOW() - started_at AS running_duration
                    FROM etl_run_history
                    WHERE status = 'running'
                      AND started_at < NOW() - (
                          :timeout_minutes * INTERVAL '1 minute'
                      )
                    ORDER BY started_at
                """),
                {"timeout_minutes": timeout_minutes}
            )

            stale_runs = result.fetchall()

            if not stale_runs:
                logger.info("Зависших запусков ETL не обнаружено")
                return

            for run in stale_runs:
                logger.warning(
                    f"Обнаружен подозрительный запуск ETL: "
                    f"run_id={run.run_id}, "
                    f"длительность={run.running_duration}"
                )

    finally:
        engine.dispose()


def create_etl_monitoring_view():
    logger.info("Создание аналитической витрины мониторинга ETL")

    engine = get_engine()

    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    CREATE OR REPLACE VIEW etl_monitoring AS
                    SELECT
                        COUNT(*) AS total_runs,

                        COUNT(*) FILTER (
                            WHERE status = 'success'
                        ) AS successful_runs,

                        COUNT(*) FILTER (
                            WHERE status = 'failed'
                        ) AS failed_runs,

                        COUNT(*) FILTER (
                            WHERE status = 'running'
                        ) AS running_runs,

                        ROUND(
                            100.0 * COUNT(*) FILTER (
                                WHERE status = 'success'
                            ) / NULLIF(COUNT(*), 0),
                            2
                        ) AS success_rate,

                        ROUND(
                            AVG(
                                EXTRACT(
                                    EPOCH FROM (finished_at - started_at)
                                )
                            ) FILTER (
                                WHERE status = 'success'
                            )::numeric,
                            2
                        ) AS avg_duration_seconds,

                        SUM(rows_loaded) FILTER (
                            WHERE status = 'success'
                        ) AS total_rows_loaded

                    FROM etl_run_history
                """)
            )

        logger.info("Витрина etl_monitoring успешно создана")

    finally:
        engine.dispose()


def create_etl_health_view():
    logger.info("Создание витрины состояния ETL")

    engine = get_engine()

    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    CREATE OR REPLACE VIEW etl_health AS
                    WITH last_run AS (
                        SELECT
                            run_id,
                            status,
                            finished_at
                        FROM etl_run_history
                        ORDER BY run_id DESC
                        LIMIT 1
                    ),
                    health AS (
                        SELECT
                            (SELECT status FROM last_run) AS last_status,

                            (
                                SELECT COUNT(*)
                                FROM etl_run_history
                                WHERE status = 'running'
                                  AND started_at < NOW()
                                      - INTERVAL '10 minutes'
                            ) AS stale_runs,

                            (
                                SELECT MAX(finished_at)
                                FROM etl_run_history
                                WHERE status = 'success'
                            ) AS last_success_at
                    )
                    SELECT
                        last_status,
                        stale_runs,
                        last_success_at,
                        CASE
                            WHEN last_status IS NULL
                                THEN 'NO_DATA'
                            WHEN stale_runs > 0
                                THEN 'WARNING'
                            WHEN last_status = 'failed'
                                THEN 'ERROR'
                            WHEN last_status = 'running'
                                THEN 'RUNNING'
                            WHEN last_success_at IS NULL
                              OR last_success_at < NOW()
                                  - INTERVAL '24 hours'
                                THEN 'WARNING'
                            ELSE 'OK'
                        END AS health_status
                    FROM health
                """)
            )

        logger.info("Витрина etl_health успешно создана")

    finally:
        engine.dispose()


def check_etl_health():
    logger.info("Проверка общего состояния ETL")

    engine = get_engine()

    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("""
                    SELECT
                        last_status,
                        stale_runs,
                        last_success_at,
                        health_status
                    FROM etl_health
                """)
            )

            health = result.mappings().one()

            logger.info(
                f"Состояние ETL: {health['health_status']}"
            )

            logger.info(
                f"Последний запуск: {health['last_status']}"
            )

            logger.info(
                f"Зависших запусков: {health['stale_runs']}"
            )

            if health["health_status"] == "OK":
                logger.info("Мониторинг ETL: проблем не обнаружено")
            elif health["health_status"] == "RUNNING":
                logger.info("Мониторинг ETL: выполняется запуск")
            elif health["health_status"] == "WARNING":
                logger.warning("Мониторинг ETL: обнаружено предупреждение")
            else:
                logger.error(
                    f"Мониторинг ETL: состояние "
                    f"{health['health_status']}"
                )

            return health["health_status"]

    finally:
        engine.dispose()


def main():
    logger.info("ETL-пайплайн запущен")

    run_id = None
    rows_extracted = None
    rows_loaded = None

    try:
        # 1. Проверка подключения к PostgreSQL
        check_database_connection()
        check_stale_etl_runs(timeout_minutes=10)
        # 2. Регистрация запуска ETL
        run_id = start_etl_run()

        logger.info(
            f"Текущий идентификатор запуска: {run_id}"
        )
        
        # 3. EXTRACT — загрузка исходных данных
        logger.info("Начинается этап EXTRACT")
        raw_data = extract()
        rows_extracted = len(raw_data)

        # 4. Отчёт о качестве исходных данных
        data_quality_report(raw_data)

        # 5. TRANSFORM — очистка данных
        logger.info("Начинается этап TRANSFORM")
        clean_data = transform(raw_data)

        # 6. Проверка очищенных данных
        validate_data(clean_data)

        # 7. Сохранение очищенного CSV
        logger.info("Начинается сохранение CSV")
        load(clean_data)

        # 8. Режим полной синхронизации
        full_snapshot = (
            os.getenv("FULL_SNAPSHOT", "false").lower() == "true"
        )

        logger.info(
            f"Режим полной синхронизации: {full_snapshot}"
        )

        # 9. Транзакционная загрузка в PostgreSQL
        logger.info(
            "Начинается транзакционная загрузка в PostgreSQL"
        )

        engine = get_engine()

        try:
            with engine.begin() as connection:

                # Проверка полноты ДО UPSERT
                snapshot_check_passed = (
                    check_snapshot_completeness(
                        clean_data,
                        connection
                    )
                )

                # UPSERT заказов
                logger.info("Начинается UPSERT заказов")

                load_to_postgres(
                    clean_data,
                    connection
                )

                # Разрешение Soft Delete
                allow_soft_delete = (
                    full_snapshot and snapshot_check_passed
                )

                logger.info(
                    f"Soft Delete разрешён: {allow_soft_delete}"
                )

                logger.info(
                    "Начинается синхронизация активности"
                )

                soft_delete_missing_orders(
                    clean_data,
                    connection,
                    full_snapshot=allow_soft_delete
                )

            logger.info(
                "Транзакция успешно завершена — COMMIT"
            )

            # Считаем строки после успешной загрузки
            rows_loaded = len(clean_data)

        finally:
            engine.dispose()

        # 10. Создание аналитической витрины
        logger.info(
            "Начинается создание аналитической витрины"
        )

        create_data_mart()
        create_etl_monitoring_view()
        create_etl_health_view()
        # 11. Фиксация успешного запуска
        
        finish_etl_run(
         run_id=run_id,
         status="success",
         rows_extracted=rows_extracted,
         rows_loaded=rows_loaded
        )

# Проверяем состояние ETL после завершения запуска
        check_etl_health()

        logger.info("ETL-пайплайн успешно завершён!")


    except Exception as error:
        logger.exception(
            "Ошибка при выполнении ETL-пайплайна"
        )

        # Фиксация неуспешного запуска
        if run_id is not None:
            try:
                finish_etl_run(
                    run_id=run_id,
                    status="failed",
                    rows_extracted=rows_extracted,
                    rows_loaded=rows_loaded,
                    error_message=str(error)
                )
            except Exception:
                logger.exception(
                    "Не удалось обновить историю запуска ETL"
                )

        raise


if __name__ == "__main__":
    main()



