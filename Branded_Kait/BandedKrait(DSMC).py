import os
import logging
import subprocess
import psutil
from configparser import ConfigParser
from datetime import datetime, timedelta

# Получаем путь к директории, где находится скрипт
script_dir = os.path.dirname(os.path.realpath(__file__))

# Путь к конфигурации, логам, очереди и обработанным статусам
config_file = os.path.join(script_dir, 'config.ini')
LOG_DIR = os.path.join(script_dir, 'logs')
QUEUE_FILE = os.path.join(script_dir, 'queue.txt')
PROCESSED_STATUSES = os.path.join(script_dir, 'processed_statuses.txt')  # Новый файл
ZABBIX_STATUS_FILE = os.path.join(script_dir, 'zabbix_status.txt')  # Файл статуса для Zabbix

# Загрузка конфигурации
config = ConfigParser()
config.read(config_file)

# Директории и параметры из конфигурации
BACKUP_DIR = config.get('Paths', 'backup_dir')
ARCHIVE_DIR = config.get('Paths', 'archive_dir')
TARGET_DIR = config.get('Paths', 'target_dir', fallback="/tmp/target")
LOG_RETENTION_DAYS = config.getint('Settings', 'log_retention_days', fallback=90)

# Параметры Zabbix
ZABBIX_ENABLED = config.getboolean('Zabbix', 'enabled', fallback=False)
ZABBIX_HOSTNAME = config.get('Zabbix', 'hostname', fallback='localhost')
ZABBIX_SERVER = config.get('Zabbix', 'server', fallback='127.0.0.1')


def setup_logging():
    """Настройка логирования"""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_file = os.path.join(LOG_DIR, f"script-{datetime.now().strftime('%Y-%m-%d')}.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def kill_previous_instances():
    """Завершение предыдущих итераций скрипта"""
    current_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        if proc.info['cmdline'] and __file__ in proc.info['cmdline']:
            if proc.info['pid'] != current_pid:
                proc.kill()
                logging.info(f"Завершен процесс с PID {proc.info['pid']} (предыдущая итерация скрипта).")


def clear_old_files():
    """Удаление старых файлов логов"""
    now = datetime.now()
    cutoff_date = now - timedelta(days=LOG_RETENTION_DAYS)

    for filename in os.listdir(LOG_DIR):
        file_path = os.path.join(LOG_DIR, filename)
        if os.path.isfile(file_path):
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if file_time < cutoff_date:
                try:
                    os.remove(file_path)
                    logging.info(f"Удален старый лог: {filename}")
                except Exception as e:
                    logging.error(f"Ошибка удаления файла {filename}: {e}")


def update_zabbix_status(file_name=None, progress=None, status=None, script_status=None):
    """
    Обновляет файл статуса для Zabbix.
    :param file_name: Имя файла/директории, которая передаётся (может быть None для статуса скрипта).
    :param progress: Прогресс передачи (в процентах, может быть None).
    :param status: Статус передачи файла/директории (например, 'в процессе', 'завершено', 'ошибка').
    :param script_status: Статус выполнения скрипта (например, 'running', 'complete', 'error').
    """
    try:
        # Проверяем, существует ли файл, если нет - создаём
        if not os.path.exists(ZABBIX_STATUS_FILE):
            with open(ZABBIX_STATUS_FILE, 'w') as f:
                f.write("")  # Создаём пустой файл

        # Обновляем файл статуса
        with open(ZABBIX_STATUS_FILE, 'w') as f:
            if script_status:
                # Запись статуса скрипта
                f.write(f"Script Status: {script_status}\n")
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            if file_name and progress is not None and status:
                # Запись статуса текущего файла/директории
                f.write(f"File: {file_name}\n")
                f.write(f"Progress: {progress}%\n")
                f.write(f"Status: {status}\n")
        logging.info(f"Обновлён статус для Zabbix: {script_status or ''} {file_name or ''} {progress or ''}% {status or ''}")
    except Exception as e:
        logging.error(f"Ошибка обновления статуса для Zabbix: {e}")


def send_to_zabbix(key, value):
    """Отправка данных в Zabbix"""
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
    """Проверка статусного файла и добавление в очередь"""
    status_file = os.path.join(repo, stanza, 'rsync.status')
    if not os.path.exists(status_file):
        return

    # Читаем содержимое статусного файла
    with open(status_file, 'r') as f:
        status_content = f.readline().strip()

    # Уникальная запись для проверок
    status_entry = f"{status_file} | {status_content}"

    # Проверяем, был ли этот статусный файл уже обработан
    if os.path.exists(PROCESSED_STATUSES):
        with open(PROCESSED_STATUSES, 'r') as processed:
            if status_entry in processed.read().splitlines():
                logging.info(f"Статусный файл уже обработан: {status_entry}")
                return

    # Если статус "complete", добавляем в очередь
    if status_content.startswith('complete'):
        with open(QUEUE_FILE, 'a') as queue:
            queue.write(f"{stanza}\n")
        logging.info(f"{stanza} добавлена в очередь на запись.")

        # Запоминаем, что этот статусный файл обработан
        with open(PROCESSED_STATUSES, 'a') as processed:
            processed.write(f"{status_entry}\n")

        send_to_zabbix("backup_script.queue_update", f"{stanza} добавлена в очередь")


def process_queue():
    """Обработка очереди заданий"""
    if not os.path.exists(QUEUE_FILE):
        return

    remaining_stanzas = []
    with open(QUEUE_FILE, 'r') as f:
        stanzas = f.read().splitlines()

    for stanza in stanzas:
        backup_path = os.path.join(BACKUP_DIR, stanza)
        archive_path = os.path.join(ARCHIVE_DIR, stanza)

        if not os.path.exists(backup_path) or not os.path.exists(archive_path):
            logging.error(f"Отсутствует необходимая директория для {stanza}: backup или archive")
            remaining_stanzas.append(stanza)
            send_to_zabbix(f"backup_script.write_status[{stanza}]", "ERROR")
            continue

        send_to_zabbix(f"backup_script.write_status[{stanza}]", "WRITING")
        success = write_to_target(stanza, backup_path, archive_path)
        if not success:
            remaining_stanzas.append(stanza)
            send_to_zabbix(f"backup_script.write_status[{stanza}]", "ERROR")
        else:
            send_to_zabbix(f"backup_script.write_status[{stanza}]", "SUCCESS")
            logging.info(f"Запись для {stanza} завершена успешно.")

    # Обновляем очередь, оставляя только оставшиеся задачи
    with open(QUEUE_FILE, 'w') as f:
        for stanza in remaining_stanzas:
            f.write(f"{stanza}\n")


def write_to_target(stanza, backup_dir, archive_dir):
    """Копирование данных в целевую директорию с использованием dsmc и обновлением статуса для Zabbix"""
    try:
        backup_target = os.path.join(TARGET_DIR, stanza, 'backup')
        archive_target = os.path.join(TARGET_DIR, stanza, 'archive')

        # Создаём целевые директории, если их нет
        os.makedirs(backup_target, exist_ok=True)
        os.makedirs(archive_target, exist_ok=True)

        # Проверка наличия sudo для запуска dsmc
        if not subprocess.run("command -v dsmc", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode == 0:
            logging.error("Не найдено команду dsmc (Data Protection Manager). Программа не может работать без этой команды.")
            return False

        # Копируем backup с помощью dsmc
        dsmc_backup = f"dsmc incremental {backup_dir}/ -backup={backup_target}"
        logging.info(f"Копирование из {backup_dir} в {backup_target} с использованием dsmc")
        update_zabbix_status(backup_dir, 0, "в процессе")
        backup_result = subprocess.run(dsmc_backup, shell=True, check=True)
        update_zabbix_status(backup_dir, 50, "в процессе")

        # Копируем archive с помощью dsmc
        dsmc_archive = f"dsmc incremental {archive_dir}/ -backup={archive_target}"
        logging.info(f"Копирование из {archive_dir} в {archive_target} с использованием dsmc")
        archive_result = subprocess.run(dsmc_archive, shell=True, check=True)
        update_zabbix_status(archive_dir, 100, "завершено")

        # Если обе команды завершились успешно
        if backup_result.returncode == 0 and archive_result.returncode == 0:
            logging.info(f"Данные для {stanza} успешно записаны.")
            return True
        else:
            update_zabbix_status(stanza, 100, "ошибка")
            logging.error(f"Ошибка записи для {stanza}: dsmc завершился с ошибкой.")
            return False
    except subprocess.CalledProcessError as e:
        update_zabbix_status(stanza, 0, "ошибка")
        logging.error(f"Ошибка при выполнении dsmc для {stanza}: {e}")
        return False


if __name__ == "__main__":
    # Завершаем предыдущие итерации скрипта
    kill_previous_instances()

    # Очищаем очередь перед запуском
    open(QUEUE_FILE, 'w').close()

    setup_logging()
    clear_old_files()
    send_to_zabbix("backup_script.status", "STARTED")

    repositories = [BACKUP_DIR]
    for repo in repositories:
        for stanza in os.listdir(repo):
            check_rsync_status(repo, stanza)

    process_queue()

    send_to_zabbix("backup_script.status", "COMPLETED")
