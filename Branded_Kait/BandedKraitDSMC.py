import os
import logging
from datetime import datetime
from utils import load_config, setup_logging, kill_previous_instances, clear_old_files, send_to_zabbix, check_rsync_status, process_queue

if __name__ == "__main__":
    # Путь к конфигу
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.ini')

    try:
        # Загрузка конфигурации
        config = load_config(config_path)
        BACKUP_DIR = config.get('Paths', 'backup_dir')
        ARCHIVE_DIR = config.get('Paths', 'archive_dir')
        TARGET_DIR = config.get('Paths', 'target_dir')
        LOG_RETENTION_DAYS = config.getint('Settings', 'log_retention_days', fallback=90)  # По умолчанию 90 дней
        ZABBIX_ENABLED = config.getboolean('Zabbix', 'enabled', fallback=False)
        ZABBIX_HOSTNAME = config.get('Zabbix', 'hostname', fallback='localhost')
        ZABBIX_SERVER = config.get('Zabbix', 'server', fallback='127.0.0.1')

    except FileNotFoundError:
        logging.error(f"Файл конфигурации не найден: {config_path}")
        exit(1)
    except Exception as e:
        logging.error(f"Ошибка при загрузке конфигурации: {e}")
        exit(1)

    # Получаем текущую дату и время
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Скрипт запущен в {current_time}")

    # Завершаем старые процессы, если есть
    kill_previous_instances()

    # Очищаем очередь перед выполнением
    with open('queue.txt', 'w'):
        pass

    # Настройка логирования
    setup_logging()

    # Удаляем старые лог-файлы
    clear_old_files(LOG_RETENTION_DAYS)

    # Отправляем в Zabbix стартовый статус
    send_to_zabbix(ZABBIX_ENABLED, ZABBIX_SERVER, ZABBIX_HOSTNAME, "backup_script.status", "STARTED")

    # Список репозиториев для бэкапа
    repositories = [BACKUP_DIR]
    for repo in repositories:
        for stanza in os.listdir(repo):
            check_rsync_status(repo, stanza)

    # Обработка очереди
    process_queue()

    # Отправляем в Zabbix финальный статус
    send_to_zabbix(ZABBIX_ENABLED, ZABBIX_SERVER, ZABBIX_HOSTNAME, "backup_script.status", "COMPLETED")
