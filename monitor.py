
import logging
import sys

from pipeline import check_etl_health



from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / "monitor.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ],
    force=True
)



def main():
    logging.info("Запуск независимого мониторинга ETL")

    try:
        status = check_etl_health()

        if status == "OK":
            logging.info("ETL работает нормально")
            return 0

        logging.warning(
            f"Обнаружена проблема с ETL: {status}"
        )
        return 1

    except Exception:
        logging.exception(
            "Не удалось проверить состояние ETL"
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
