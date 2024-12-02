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
LENTOCHKA_DIR = os.path.join(script_dir, 'lentochka')


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
    """Проверка статусных файлов в директориях backup и archive, добавление в очередь."""
    backup_path = os.path.join(repo, 'backup', stanza)
    archive_path = os.path.join(repo, 'archive', stanza)

    # Проверка наличия обоих каталогов
    if not (os.path.exists(backup_path) and os.path.exists(archive_path)):
        logging.warning(f"Отсутствует один из необходимых каталогов для {stanza} (backup или archive)")
        return False

    # Проверка существующих lentochka-status файлов
    backup_status_file = os.path.join(backup_path, 'lentochka-status')
    archive_status_file = os.path.join(archive_path, 'lentochka-status')
    
    if os.path.exists(backup_status_file) or os.path.exists(archive_status_file):
        logging.info(f"Бэкап для {stanza} уже обработан (найден lentochka-status)")
        return False

    # Проверка rsync.status
    rsync_status_file = os.path.join(backup_path, 'rsync.status')
    if not os.path.exists(rsync_status_file):
        logging.warning(f"Статусный файл rsync.status не найден: {rsync_status_file}")
        return False

    with open(rsync_status_file, 'r') as f:
        status_content = f.readline().strip()

    if not status_content.startswith('complete'):
        logging.info(f"Статус для {stanza} не complete: {status_content}")
        return False

    logging.info(f"Статусный файл для {stanza} подтверждает завершение копирования")
    return True


def handle_repo_files(backup_dir='lentochka'):
    """Обработка файлов с расширением .repo."""
    for root, dirs, files in os.walk(script_dir):
        # Пропускаем системные директории
        dirs[:] = [d for d in dirs if not d.startswith(('sys', 'proc', 'dev', 'run'))]
        
        for dir in dirs:
            if dir.endswith(".repo"):
                repo_dir = os.path.join(root, dir)
                logging.info(f"Обрабатываем каталог репозитория: {repo_dir}")

                # Проверяем наличие обоих каталогов
                backup_path = os.path.join(repo_dir, 'backup')
                archive_path = os.path.join(repo_dir, 'archive')

                if not (os.path.exists(backup_path) and os.path.exists(archive_path)):
                    logging.warning(f"Пропуск {repo_dir}: отсутствует backup или archive директория")
                    continue

                try:
                    # Проверяем все станзы в backup директории
                    for stanza in os.listdir(backup_path):
                        stanza_backup_path = os.path.join(backup_path, stanza)
                        stanza_archive_path = os.path.join(archive_path, stanza)
                        
                        # Пропускаем, если это не директория
                        if not os.path.isdir(stanza_backup_path):
                            continue
                            
                        if check_rsync_status(repo_dir, stanza):
                            logging.info(f"Добавляем {stanza} в очередь на копирование")
                            with open(QUEUE_FILE, 'a') as queue:
                                queue.write(f"{repo_dir}:{stanza}\n")
                except Exception as e:
                    logging.error(f"Ошибка при обработке репозитория {repo_dir}: {str(e)}")
                    continue


def process_queue(queue_file='queue.txt'):
    """Обработка очереди задач."""
    if not os.path.exists(queue_file):
        logging.warning("Очередь пуста.")
        return
    with open(queue_file, 'r') as queue:
        for line in queue.readlines():
            repo_dir, stanza = line.strip().split(':')
            logging.info(f"Обработка стана: {stanza}")
            # Логика обработки .repo файлов


def create_lentochka_dir():
    """Создание директории на ленте."""
    if not os.path.exists(LENTOCHKA_DIR):
        try:
            os.makedirs(LENTOCHKA_DIR)
            logging.info(f"Создана директория на ленте: {LENTOCHKA_DIR}")
        except Exception as e:
            logging.error(f"Ошибка создания директории на ленте: {e}")


def backup_with_dsmc():
    """Резервное копирование с использованием dsmc."""
    if not os.path.exists(QUEUE_FILE):
        logging.info("Очередь пуста")
        return

    with open(QUEUE_FILE, 'r') as queue:
        for line in queue:
            repo_dir, stanza = line.strip().split(':')
            backup_path = os.path.join(repo_dir, 'backup', stanza)
            archive_path = os.path.join(repo_dir, 'archive', stanza)

            try:
                # Создаем директорию на ленте, если её нет
                create_lentochka_dir()

                # Копируем backup
                logging.info(f"Копирование backup для {stanza} на ленту")
                cmd = ["dsmc", "archive", backup_path, "-description=backup"]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    raise Exception(f"Ошибка при копировании backup: {result.stderr}")

                # Копируем archive
                logging.info(f"Копирование archive для {stanza} на ленту")
                cmd = ["dsmc", "archive", archive_path, "-description=archive"]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    raise Exception(f"Ошибка при копировании archive: {result.stderr}")

                # Создаем статусные файлы после успешного копирования
                for path in [backup_path, archive_path]:
                    status_file = os.path.join(path, 'lentochka-status')
                    with open(status_file, 'w') as f:
                        f.write(f"complete;{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                logging.info(f"Успешно скопирован бэкап {stanza} на ленту")
                
                # Обновляем статус в Zabbix
                if ZABBIX_ENABLED:
                    update_zabbix_status(stanza, status="complete")

            except Exception as e:
                logging.error(f"Ошибка при копировании {stanza} на ленту: {str(e)}")
                if ZABBIX_ENABLED:
                    update_zabbix_status(stanza, status="error")
                continue


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
        BACKUP_DIR, ARCHIVE_DIR, TARGET_DIR, LOG_RETENTION_DAYS, ZABBIX_ENABLED, ZABBIX_HOSTNAME, ZABBIX_SERVER = load_config(
            config_path)

        handle_repo_files()
        process_queue()
        backup_with_dsmc()
    except Exception as e:
        logging.error(f"Ошибка в процессе выполнения скрипта: {e}")
