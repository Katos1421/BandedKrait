import os
import logging
import subprocess
from configparser import ConfigParser
from datetime import datetime
import time

# Загрузка конфигурации
config = ConfigParser()
config.read('config.ini')

# Директории и параметры из конфигурации
LOG_DIR = config.get('Paths', 'log_dir', fallback='logs')
BACKUP_DIR = config.get('Paths', 'backup_dir')
ARCHIVE_DIR = config.get('Paths', 'archive_dir')
QUEUE_FILE = config.get('Paths', 'queue_file', fallback='queue.txt')
LOG_RETENTION_DAYS = config.getint('Settings', 'log_retention_days', fallback=90)

# Параметры Zabbix
ZABBIX_ENABLED = config.getboolean('Zabbix', 'enabled', fallback=False)
ZABBIX_HOSTNAME = config.get('Zabbix', 'hostname', fallback='localhost')
ZABBIX_SERVER = config.get('Zabbix', 'server', fallback='127.0.0.1')

# Настройка логирования
def setup_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_file = os.path.join(LOG_DIR, f"script-{datetime.now().strftime('%Y-%m-%d')}.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

def clear_old_logs():
    now = datetime.now()
    for filename in os.listdir(LOG_DIR):
        file_path = os.path.join(LOG_DIR, filename)
        if os.path.isfile(file_path):
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if (now - file_time).days > LOG_RETENTION_DAYS:
                try:
                    os.remove(file_path)
                    logging.info(f"Удален старый лог: {filename}")
                except Exception as e:
                    logging.error(f"Ошибка удаления файла {filename}: {e}")

def send_to_zabbix(key, value):
    """Отправка данных в Zabbix с использованием zabbix_sender"""
    command = f"zabbix_sender -z {ZABBIX_SERVER} -s {ZABBIX_HOSTNAME} -k {key} -o {value}"
    try:
        result = subprocess.run(command, shell=True, check=True)
        if result.returncode != 0:
            logging.error(f"Ошибка отправки данных в Zabbix: {key} = {value}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при отправке данных в Zabbix: {e}")

def check_rsync_status(repo, stanza):
    status_file = os.path.join(repo, 'backup', stanza, 'rsync.status')
    if not os.path.exists(status_file):
        return
    with open(status_file, 'r') as f:
        status = f.readline().strip().split(';')[0]
    if status == 'complete':
        with open(QUEUE_FILE, 'a') as queue:
            queue.write(f"{stanza}\n")
        logging.info(f"{stanza} добавлена в очередь на запись на ленту.")
        if ZABBIX_ENABLED:
            send_to_zabbix("backup_script.queue_update", f"QUEUE_UPDATED: {len(open(QUEUE_FILE).readlines())}")

def process_queue():
    if not os.path.exists(QUEUE_FILE):
        return
    remaining_stanzas = []
    with open(QUEUE_FILE, 'r') as f:
        stanzas = f.read().splitlines()
    for stanza in stanzas:
        backup_path = os.path.join(BACKUP_DIR, stanza)
        archive_path = os.path.join(ARCHIVE_DIR, stanza)
        if ZABBIX_ENABLED:
            send_to_zabbix(f"backup_script.write_status[{stanza}]", "WRITING")
        success = write_to_tape(stanza, backup_path, archive_path)
        if not success:
            remaining_stanzas.append(stanza)
            if ZABBIX_ENABLED:
                send_to_zabbix(f"backup_script.write_status[{stanza}]", "ERROR")
        elif ZABBIX_ENABLED:
            send_to_zabbix(f"backup_script.write_status[{stanza}]", "SUCCESS")

    with open(QUEUE_FILE, 'w') as f:
        for stanza in remaining_stanzas:
            f.write(f"{stanza}\n")

def write_to_tape(stanza, backup_dir, archive_dir):
    tape_command = f"dsmc backup {backup_dir} {archive_dir}"
    try:
        result = subprocess.run(tape_command, shell=True, check=True)
        if result.returncode == 0:
            logging.info(f"Запись на ленту для {stanza} завершена успешно.")
            return True
        else:
            logging.error(f"Ошибка записи на ленту для {stanza}.")
            return False
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при запуске команды dsmc: {e}")
        return False

if __name__ == "__main__":
    setup_logging()
    clear_old_logs()
    if ZABBIX_ENABLED:
        send_to_zabbix("backup_script.status", "STARTED")

    repositories = [BACKUP_DIR]
    for repo in repositories:
        for stanza in os.listdir(os.path.join(repo, "backup")):
            check_rsync_status(repo, stanza)

    process_queue()

    if ZABBIX_ENABLED:
        send_to_zabbix("backup_script.status", "COMPLETED")