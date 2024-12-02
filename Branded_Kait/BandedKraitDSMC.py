import os
import logging
import subprocess
import psutil
from datetime import datetime, timedelta
from configparser import ConfigParser
import argparse

# Получаем путь к директории, где находится скрипт
script_dir = os.path.dirname(os.path.realpath(__file__))

# Путь к конфигурации, логам, очереди и обработанным статусам
LOG_DIR = os.path.join(script_dir, 'logs')
ZABBIX_STATUS_FILE = os.path.join(script_dir, 'zabbix_status.txt')  # Файл статуса для Zabbix
QUEUE_FILE = os.path.join(script_dir, 'queue.txt')


def parse_arguments():
    """Парсинг аргументов командной строки для режима verbose."""
    parser = argparse.ArgumentParser(description="Backup script.")
    parser.add_argument('--verbose', action='store_true', help="Enable verbose logging")
    return parser.parse_args()


def load_config(config_path):
    """Загрузка конфигурации из файла."""
    config = ConfigParser()
    if not os.path.exists(config_path):
        logging.error(f"Файл конфигурации {config_path} не найден.")
        raise FileNotFoundError(f"Файл конфигурации {config_path} отсутствует.")

    config.read(config_path)
    try:
        BACKUP_DIR = config.get('Paths', 'backup_dir')
        ARCHIVE_DIR = config.get('Paths', 'archive_dir')
        TARGET_DIR = config.get('Paths', 'target_dir', fallback="/tmp/target")
        LOG_RETENTION_DAYS = config.getint('Settings', 'log_retention_days', fallback=90)
        ZABBIX_ENABLED = config.getboolean('Zabbix', 'enabled', fallback=False)
        ZABBIX_HOSTNAME = config.get('Zabbix', 'hostname', fallback='localhost')
        ZABBIX_SERVER = config.get('Zabbix', 'server', fallback='127.0.0.1')

        return BACKUP_DIR, ARCHIVE_DIR, TARGET_DIR, LOG_RETENTION_DAYS, ZABBIX_ENABLED, ZABBIX_HOSTNAME, ZABBIX_SERVER
    except Exception as e:
        logging.error(f"Ошибка при загрузке конфигурации: {e}")
        raise


def setup_logging(level=logging.INFO):
    """Настройка логирования."""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_file = os.path.join(LOG_DIR, f"script-{datetime.now().strftime('%Y-%m-%d')}.log")
    logging.basicConfig(
        filename=log_file,
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def kill_previous_instances():
    """Завершение предыдущих итераций скрипта."""
    current_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        if proc.info['cmdline'] and __file__ in proc.info['cmdline']:
            if proc.info['pid'] != current_pid:
                try:
                    proc.kill()
                    logging.info(f"Завершён процесс с PID {proc.info['pid']} (предыдущая итерация скрипта).")
                except psutil.NoSuchProcess:
                    logging.error(f"Процесс с PID {proc.info['pid']} уже завершён.")


def clear_old_files(LOG_RETENTION_DAYS):
    """Удаление старых файлов логов."""
    now = datetime.now()
    cutoff_date = now - timedelta(days=LOG_RETENTION_DAYS)
    for filename in os.listdir(LOG_DIR):
        file_path = os.path.join(LOG_DIR, filename)
        if os.path.isfile(file_path):
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if file_time < cutoff_date:
                try:
                    os.remove(file_path)
                    logging.info(f"Удалён старый лог: {filename}")
                except Exception as e:
                    logging.error(f"Ошибка удаления файла {filename}: {e}")


def update_zabbix_status(file_name=None, progress=None, status=None, script_status=None):
    """Обновляет файл статуса для Zabbix."""
    try:
        with open(ZABBIX_STATUS_FILE, 'w') as f:
            if script_status:
                f.write(f"Script Status: {script_status}\n")
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            if file_name and progress is not None and status:
                f.write(f"File: {file_name}\n")
                f.write(f"Progress: {progress}%\n")
                f.write(f"Status: {status}\n")
        logging.info(
            f"Обновлён статус для Zabbix: {script_status or ''} {file_name or ''} {progress or ''}% {status or ''}")
    except Exception as e:
        logging.error(f"Ошибка обновления статуса для Zabbix: {e}")


def send_to_zabbix(ZABBIX_ENABLED, ZABBIX_SERVER, ZABBIX_HOSTNAME, key, value):
    """Отправка данных в Zabbix."""
    if not ZABBIX_ENABLED:
        return
    command = f"zabbix_sender -z {ZABBIX_SERVER} -s {ZABBIX_HOSTNAME} -k {key} -o {value}"
    try:
        result = subprocess.run(command, shell=True, check=True)
        if result.returncode != 0:
            logging.error(f"Ошибка отправки данных в Zabbix: {key} = {value}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при отправке данных в Zabbix: {e}")


def check_rsync_status(repo, stanza):
    """Проверка статусного файла и добавление в очередь."""
    status_file = os.path.join(repo, stanza, 'rsync.status')
    lentochka_status_file = os.path.join(repo, stanza, 'lentochka-status')  # Новый файл статуса
    if not os.path.exists(status_file):
        logging.warning(f"Статусный файл не найден: {status_file}")
        return
    with open(status_file, 'r') as f:
        status_content = f.readline().strip()

    # Проверка наличия нового статусного файла
    if os.path.exists(lentochka_status_file):
        logging.info(f"Бэкап для {stanza} уже обработан.")
        return

    if status_content.startswith('complete'):
        with open(QUEUE_FILE, 'a') as queue:
            queue.write(f"{stanza}\n")
        logging.info(f"{stanza} добавлена в очередь на запись.")
        with open(lentochka_status_file, 'w') as status:
            status.write(f"Processed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        send_to_zabbix(True, "127.0.0.1", "localhost", "backup_script.queue_update", f"{stanza} добавлена в очередь")


def process_queue():
    """Обработка очереди задач."""
    if not os.path.exists(QUEUE_FILE):
        logging.warning("Очередь пуста.")
        return
    with open(QUEUE_FILE, 'r') as queue:
        for line in queue.readlines():
            stanza = line.strip()
            logging.info(f"Обработка стана: {stanza}")
            # Добавь логику обработки
    open(QUEUE_FILE, 'w').close()  # Очистить очередь


if __name__ == "__main__":
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.ini')
    try:
        args = parse_arguments()
        if args.verbose:
            setup_logging(logging.DEBUG)
        else:
            setup_logging()

        logging.info(f"Скрипт запущен в {start_time}")
        kill_previous_instances()
        clear_old_files(LOG_RETENTION_DAYS)
        send_to_zabbix(ZABBIX_ENABLED, ZABBIX_SERVER, ZABBIX_HOSTNAME, "backup_script.status", "STARTED")
    except Exception as e:
        logging.error(f"Проблема запуска: {e}")
