#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import signal
import logging
import subprocess
import configparser
import time
import psutil
import datetime

class DsmcPlusLentochkaLogs:
    """
    Класс для управления логированием в приложении.
    Включает глобальный логгер и отдельный логгер для DSMC.
    """

    def __init__(self, lentochka_log_dir, dsmc_log_dir):
        """
        Инициализация класса с указанием директорий для логов.

        :param lentochka_log_dir: Директория для глобального логгера (lentochka.log).
        :param dsmc_log_dir: Директория для логгера DSMC.
        """
        self.lentochka_log_dir = self.resolve_relative_path(lentochka_log_dir)
        self.dsmc_log_dir = self.resolve_relative_path(dsmc_log_dir)

        self.logger = logging.getLogger()
        self.dsmc_logger = logging.getLogger('dsmc')

        self.global_lentochka_log_file = os.path.join(self.lentochka_log_dir, 'global-lentochka.log')
        self.global_dsmc_log_file = os.path.join(self.dsmc_log_dir, 'global-dsmc.log')

        self.logger.info(f"Абсолютный путь к lentochka_log_dir: {self.lentochka_log_dir}")
        self.logger.info(f"Абсолютный путь к dsmc_log_dir: {self.dsmc_log_dir}")

        self._setup_lentochka_logger()
        self._setup_dsmc_logger()

    @staticmethod
    def resolve_relative_path(path):
        """
        Преобразует относительный путь в абсолютный относительно корня проекта.
        Если путь уже абсолютный, возвращает его без изменений.

        :param path: Путь к директории или файлу.
        :return: Абсолютный путь.
        """
        if os.path.isabs(path):
            return path
        project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
        return os.path.abspath(os.path.join(project_root, path))

    def _setup_lentochka_logger(self):
        """
        Настройка глобального логгера для приложения (lentochka).
        """
        # Очистка обработчиков
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Проверка и создание директории логов
        if not os.path.exists(self.lentochka_log_dir):
            os.makedirs(self.lentochka_log_dir, exist_ok=True)

        # Настройка логгера для глобального лога
        log_file_path = os.path.join(self.lentochka_log_dir, 'lentochka_global_log_file.log')
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        time_format = '%Y-%m-%d %H:%M:%S'

        # Создаем обработчик для глобального лога
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(logging.Formatter(log_format, datefmt=time_format))
        self.logger.addHandler(file_handler)

        # Создаем обработчик для логов с таймштампом
        timestamp_log_file_path = os.path.join(self.lentochka_log_dir,
                                               f'lentochka_log_{datetime.datetime.now().strftime("%Y-%m-%d")}.log')
        timestamp_file_handler = logging.FileHandler(timestamp_log_file_path)
        timestamp_file_handler.setFormatter(logging.Formatter(log_format, datefmt=time_format))
        self.logger.addHandler(timestamp_file_handler)

        # Добавление обработчика для консоли
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter(log_format, datefmt=time_format))
        self.logger.addHandler(console_handler)

        self.logger.info(f"Глобальный логгер настроен. Логи пишутся в файл: {log_file_path}")

    def _setup_dsmc_logger(self):
        """
        Настройка логгера для DSMC.
        """
        # Очистка обработчиков
        if self.dsmc_logger.hasHandlers():
            self.dsmc_logger.handlers.clear()

        # Проверка директории логов
        self.validate_dsmc_log_dir()

        # Настройка логгера DSMC (глобальный лог)
        log_file_path = os.path.join(self.dsmc_log_dir, 'dsmc_global_log_file.log')
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        time_format = '%Y-%m-%d %H:%M:%S'

        # Создаем обработчик для глобального лога DSMC
        file_handler = logging.FileHandler(log_file_path, mode='a')
        file_handler.setFormatter(logging.Formatter(log_format, datefmt=time_format))
        self.dsmc_logger.addHandler(file_handler)

        # Создаем обработчик для лога DSMC с таймштампом
        timestamp_log_file_path = os.path.join(self.dsmc_log_dir,
                                               f'dsmc_log_{datetime.datetime.now().strftime("%Y-%m-%d")}.log')
        timestamp_file_handler = logging.FileHandler(timestamp_log_file_path)
        timestamp_file_handler.setFormatter(logging.Formatter(log_format, datefmt=time_format))
        self.dsmc_logger.addHandler(timestamp_file_handler)

        # Устанавливаем уровень логирования
        self.dsmc_logger.setLevel(logging.INFO)

        self.dsmc_logger.info(f"DSMC логгер настроен. Логи пишутся в файл: {log_file_path}")

    def log_lentochka_info(self, message):
        """
        Записывает сообщение уровня INFO в глобальный логгер.

        :param message: Текст сообщения.
        """
        self.logger.info(message)

    def log_lentochka_error(self, message):
        """
        Записывает сообщение уровня ERROR в глобальный логгер.

        :param message: Текст сообщения.
        """
        self.logger.error(message)

    def log_dsmc_info(self, message):
        """
        Записывает сообщение уровня INFO в DSMC логгер.

        :param message: Текст сообщения.
        """
        self.dsmc_logger.info(message)

    def log_dsmc_error(self, message):
        """
        Записывает сообщение уровня ERROR в DSMC логгер.

        :param message: Текст сообщения.
        """
        self.dsmc_logger.error(message)

    def cleanup_logs(self, retention_days):
        """
        Удаляет логи старше указанного количества дней в обоих лог-директориях.

        :param retention_days: Количество дней для хранения логов.
        """
        self._cleanup_directory(self.lentochka_log_dir, retention_days, "lentochka")
        self._cleanup_directory(self.dsmc_log_dir, retention_days, "dsmc")

    def _cleanup_directory(self, directory, retention_days, log_type):
        """
        Вспомогательный метод для очистки логов в указанной директории.

        :param directory: Директория для очистки.
        :param retention_days: Количество дней для хранения логов.
        :param log_type: Тип логов (для логирования процесса очистки).
        """
        if not os.path.exists(directory):
            self.logger.warning(f"{log_type.capitalize()} лог директория не существует: {directory}")
            return

        deleted_files_count = 0
        now = datetime.datetime.now()

        for log_file in os.listdir(directory):
            log_file_path = os.path.join(directory, log_file)
            if os.path.isfile(log_file_path):
                file_age_days = (now - datetime.datetime.fromtimestamp(os.path.getmtime(log_file_path))).days
                if file_age_days > retention_days:
                    try:
                        os.remove(log_file_path)
                        deleted_files_count += 1
                    except Exception as ex:
                        self.logger.error(f"Ошибка при удалении файла {log_file_path}: {ex}")

        self.logger.info(f"Удалено {deleted_files_count} старых {log_type} логов из директории {directory}.")

    def validate_log_directory(self, directory):
        """
        Проверяет существование и доступность директории для логов.
        Если директория отсутствует, создаёт её.

        :param directory: Директория для проверки.
        """
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                self.logger.info(f"Директория для логов создана: {directory}")
            except PermissionError:
                self.logger.error(f"Ошибка: нет прав на создание директории {directory}.")
                raise

        if not os.access(directory, os.W_OK):
            self.logger.error(f"Ошибка: нет прав на запись в директорию {directory}.")
            raise PermissionError(f"Нет прав на запись в директорию {directory}.")

        self.logger.info(f"Директория для логов доступна: {directory}")

    def validate_dsmc_log_dir(self):
        """
        Проверяет существование и доступность директории DSMC логов.
        Если директория отсутствует, создаёт её.
        """
        self.validate_log_directory(self.dsmc_log_dir)

    def generate_dsmc_log_file(self, repo_path):
        """
        Генерирует уникальный путь для DSMC лог-файла.

        :param repo_path: Путь к репозиторию.
        :return: Путь к лог-файлу.
        """
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file_name = f"dsmc-{os.path.basename(repo_path)}-{timestamp}.log"
        return os.path.join(self.dsmc_log_dir, log_file_name)

    def run_dsmc_command(self, dsmc_path, backup_path, archive_path, repo_path):
        """
        Выполняет DSMC команду для указанных директорий backup и archive с перенаправлением вывода в лог.

        :param dsmc_path: Путь к исполняемому файлу DSMC.
        :param backup_path: Путь к директории backup.
        :param archive_path: Путь к директории archive.
        :param repo_path: Путь к репозиторию (для генерации имени лог-файла).
        :return: Код выполнения команды (0 - успех, 1 - ошибка).
        """
        # Инициализируем переменную command пустым значением
        command = ""

        try:
            # Генерируем путь к лог-файлу
            log_file_path = self.generate_dsmc_log_file(repo_path)

            # Формируем команду DSMC с перенаправлением вывода в лог
            command = f'{dsmc_path} incr "{backup_path}" "{archive_path}" -su=yes >> "{log_file_path}" 2>&1'

            # Выполняем команду
            result = subprocess.run(command, shell=True, check=True)

            # Логируем успешное выполнение
            self.log_dsmc_info(f"Backup completed for {repo_path}. Log saved to {log_file_path}")
            return 0

        except subprocess.CalledProcessError as e:
            # Логируем ошибку
            self.log_dsmc_error(
                f"Error during DSMC command execution. Command: {command}. Error: {e.stderr.decode() if e.stderr else 'No stderr'}"
            )
            return 1

        except Exception as exception2:
            # Логируем любую другую ошибку
            self.log_dsmc_error(f"Unexpected error during DSMC command execution for {repo_path}: {exception2}")
            return 1


def initialize_config():
    """
    Ищет и загружает конфигурационный файл.
    Returns:
        configparser.ConfigParser: Объект конфигурации.
    """
    global CONFIG_FILE
    CONFIG_FILE = '/home/dan/PycharmProjects/Lentochnik/LentochkaDSMC.ini'

    if not os.path.exists(CONFIG_FILE):
        error_msg = f"Конфигурационный файл не найден: {CONFIG_FILE}"
        raise FileNotFoundError(error_msg)

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    return config

# Загрузка конфигурации
config = initialize_config()

# Инициализация глобального лог-менеджера с использованием конфигурационного файла
log_manager = DsmcPlusLentochkaLogs(
    lentochka_log_dir=config.get('Paths', 'lentochka_log_dir', fallback='./logs/lentochka'),
    dsmc_log_dir=config.get('DSMC', 'dsmc_log_dir', fallback='./logs/dsmc')
)

class MonitoringHandler:
    """Класс для работы с системой мониторинга."""

    def __init__(self, config):
        self.enabled = config.getboolean('Monitoring', 'enabled', fallback=False)
        self.script = config.get('Monitoring', 'monitoring_script', fallback=None)
        self.interval = config.getint('Monitoring', 'interval', fallback=300)

        # Параметры логирования
        self.log_dir = config.get('Paths', 'log_dir')
        self.log_retention_days = config.getint('Logging', 'log_retention_days', fallback=90)
        self.log_cleanup_enabled = config.getboolean('Logging', 'log_cleanup_enabled', fallback=True)

        # Добавляем логирование директории поиска
        search_root = config.get('Paths', 'search_root')
        log_manager.log_lentochka_info(f"Search directory specified in .ini file: {search_root}")

    def send_metric(self, metric_name, value, status='OK'):
        """Отправка метрики в систему мониторинга."""
        if not self.enabled or not self.script:
            log_manager.logger.warning("Monitoring is disabled or monitoring script is not set.")
            return

        try:
            cmd = [self.script, metric_name, str(value), status]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            log_manager.logger.info(f"Metric sent: {metric_name} with value: {value} and status: {status}")
        except subprocess.CalledProcessError as e:
            log_manager.logger.error(f"Error sending metric to monitoring: {e}")

    def cleanup_logs(self, log_dir, log_retention_days):
        """
        Очистка старых логов.
        """
        if not self.log_cleanup_enabled:
            log_manager.logger.info("Automatic log cleanup is disabled.")
            return 0
        if not os.path.isdir(log_dir):
            log_manager.logger.warning(f"Log directory does not exist: {log_dir}")
            return 0
        deleted_files_count = 0
        for log_file in os.listdir(log_dir):
            log_file_path = os.path.join(log_dir, log_file)
            if os.path.isfile(log_file_path):
                file_age_days = (datetime.datetime.now() - datetime.datetime.fromtimestamp(
                    os.path.getmtime(log_file_path))).days
                if file_age_days > log_retention_days:
                    try:
                        os.remove(log_file_path)
                        deleted_files_count += 1
                    except Exception as exception3:
                        log_manager.logger.error(f"Error removing file {log_file_path}: {e}")
        if deleted_files_count > 0:
            log_manager.logger.info(f"Deleted {deleted_files_count} old logs.")
        return deleted_files_count


class ProcessLocker:
    """Класс для контроля и завершения предыдущих процессов."""

    def __init__(self, lock_file_path):
        self.lock_file_path = lock_file_path
        self.pid_file = None

    def _find_existing_process(self):
        try:
            if not os.path.exists(self.lock_file_path):
                return None
            with open(self.lock_file_path, 'r') as f:
                pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
                return pid
            except OSError:
                return None
        except (IOError, ValueError) as e:
            log_manager.logger.error(f"Error reading PID from file {self.lock_file_path}: {e}")
            return None

    def terminate_existing_process(self):
        pid = self._find_existing_process()
        if pid is not None:
            log_manager.logger.warning(f"Found active process with PID {pid}. Terminating.")
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(3)
                try:
                    os.kill(pid, signal.SIGKILL)
                    log_manager.logger.warning(f"Process {pid} did not terminate, forcing termination.")
                except ProcessLookupError:
                    log_manager.logger.info(f"Process {pid} already terminated.")
                if os.path.exists(self.lock_file_path):
                    os.unlink(self.lock_file_path)
            except Exception as exception4:
                log_manager.logger.error(f"Error terminating process {pid}: {e}")
        else:
            log_manager.logger.info("No active processes to terminate.")

    def __enter__(self):
        self.terminate_existing_process()
        lock_file = self.lock_file_path
        if os.path.exists(lock_file):
            os.remove(lock_file)
        self.pid_file = open(lock_file, 'w')
        self.pid_file.write(str(os.getpid()))
        self.pid_file.flush()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.pid_file:
                self.pid_file.close()
            if os.path.exists(self.lock_file_path):
                os.unlink(self.lock_file_path)
        except Exception as e:
            log_manager.logger.error(f"Error releasing resources: {e}")


def validate_and_prepare_log_dir(log_dir):
    """
    Проверяет и подготавливает директорию для логов.
    """
    if not log_dir or log_dir.strip() == '':
        error_msg = "ОШИБКА: НЕ ЗАДАНА ДИРЕКТОРИЯ ДЛЯ ЛОГОВ В КОНФИГУРАЦИОННОМ ФАЙЛЕ!"
        log_manager.logger.error(error_msg)
        monitoring.send_metric("log_dir_error", 1, "ERROR")
        raise ValueError(error_msg)

    # Получаем абсолютный путь
    project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    log_dir = os.path.join(project_root, log_dir.strip())

        # Проверяем доступность на запись
    log_manager.validate_dsmc_log_dir()

    # Удаляем старые обработчики
    if log_manager.logger.hasHandlers():
        log_manager.logger.handlers.clear()

    # Настраиваем логгер
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    time_format = '%Y-%m-%d %H:%M:%S'
    log_file_path = os.path.join(log_dir, 'lentochka.log')

    # Добавление обработчика для вывода в консоль
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter(log_format, datefmt=time_format))
    log_manager.logger.addHandler(console_handler)

    log_manager.logger.info(f"Логирование настроено в файл: {log_file_path}")

    return log_dir

def initialize_config():
    """
    Ищет и загружает конфигурационный файл.
    Returns:
        configparser.ConfigParser: Объект конфигурации.
    """
    global CONFIG_FILE
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    log_manager.logger.info(f"Путь к проекту: {project_root}")  # Логирование пути к проекту

    # Путь к файлу конфигурации
    CONFIG_FILE = '/home/dan/PycharmProjects/Lentochnik/LentochkaDSMC.ini'

    if not os.path.exists(CONFIG_FILE):
        error_msg = f"Конфигурационный файл не найден: {CONFIG_FILE}"
        log_manager.logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    return config

def find_stanzas(config):
    """
    Ищет все станзы для обработки.
    """
    stanzas = []
    search_root = config.get('Paths', 'search_root')

    for root, dirs, files in os.walk(search_root):
        for file in files:
            if file == 'rsync.status':
                status_path = os.path.join(root, file)

                try:
                    with open(status_path, 'r') as f:
                        status_content = f.read().strip().lower()

                        if 'complete' in status_content:
                            status = 'completed'
                        elif 'failed' in status_content:
                            status = 'failed'
                        else:
                            status = 'not completed'

                        log_manager.logger.info(f"Found rsync.status file at the following path: {status_path}")
                        log_manager.logger.info(f"Status of rsync file: {status}")
                except IOError as exception:
                    log_manager.logger.error(f"Error reading file {status_path}: {exception}")
                    continue

                # Добавляем станзу в список
                stanzas.append({
                    'status_path': status_path,
                    'repo_path': root,
                    'status': status
                })

    return stanzas

def process_stanza(stanza_info, config, monitoring):
    try:
        # Проверяем директорию для логов DSMC
        log_manager.validate_dsmc_log_dir()

        # Установка времени начала
        start_time = datetime.datetime.now()

        # Проверяем наличие файла lentochka-status
        status_dir = os.path.dirname(stanza_info['status_path'])
        lentochka_status_path = os.path.join(status_dir, 'lentochka-status')
        if os.path.exists(lentochka_status_path):
            log_manager.log_lentochka_info(f"Stanza ({stanza_info['repo_path']}) already processed, skipping.")
            return True

        # Проверяем содержимое rsync.status
        with open(stanza_info['status_path'], 'r') as f:
            rsync_status = f.read().strip().lower()
        if not rsync_status.startswith("complete"):
            log_manager.log_lentochka_info(
                f"Skipping stanza ({stanza_info['repo_path']}): rsync status does not start with 'complete' (found: '{rsync_status}')."
            )
            return False

        # Обрабатываем только саму директорию .repo
        repo_path = stanza_info['repo_path']
        if not os.path.exists(repo_path):
            log_manager.log_lentochka_error(f"Skipping stanza ({repo_path}): directory does not exist.")
            return False

        # Проверка наличия папок backup и archive
        backup_path = os.path.join(repo_path, 'backup')
        archive_path = os.path.join(repo_path, 'archive')

        # Проверяем, что папки backup и archive существуют
        if not os.path.exists(backup_path) and not os.path.exists(archive_path):
            log_manager.log_lentochka_info(
                f"Skipping stanza ({repo_path}): Neither 'backup' nor 'archive' directories exist."
            )
            return False

        # Выполняем команду DSMC через класс
        dsmc_path = config.get('DSMC', 'dsmc_path', fallback='dsmc')
        result = log_manager.run_dsmc_command(dsmc_path, backup_path, archive_path, repo_path)

        # Создаем lentochka-status
        if result == 0:  # Если команда выполнена успешно
            end_time = datetime.datetime.now()
            status_content = f"Backup written to tape\nStart: {start_time.isoformat()}\nEnd: {end_time.isoformat()}"
            with open(lentochka_status_path, 'w') as f:
                f.write(status_content)

            log_manager.log_dsmc_info(
                f"Finished processing stanza {stanza_info['repo_path']} - status: {stanza_info['status']}, file lentochka-status created."
            )
            return True
        else:
            return False

    except Exception as exception6:
        log_manager.log_lentochka_error(f"Uncaught error processing stanza: {e}")
        return False

def log_error_with_metrics(message, error):
    log_manager.log_lentochka_error(f"{message}: {error}")
    try:
        if monitoring:
            monitoring.send_metric("error", 1, "ERROR")
    except Exception as e:
        log_manager.log_lentochka_error(f"Error sending metrics: {e}")

def check_write_access(directory):
    if not os.access(directory, os.W_OK):
        log_manager.logger.error(f"No write access to directory: {directory}")
        return False
    return True

def check_if_running():
    current_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['pid'] != current_pid and proc.info['name'] == 'python3':
            cmdline = proc.cmdline()
            if len(cmdline) > 0 and 'LentochkaDSMC.py' in cmdline:
                return True
    return False

def cleanup_empty_logs(log_dir):
    """
    Удаляет пустые лог-файлы.
    """
    for log_file in os.listdir(log_dir):
        if log_file.endswith('.log'):
            log_path = os.path.join(log_dir, log_file)
            if os.path.getsize(log_path) == 0:
                os.remove(log_path)
                print(f'Deleted empty log file: {log_path}')  

def sanitize_metric_name(name):
    """
    Очищает имя метрики от неподдерживаемых символов.
    """
    return name.replace(' ', '_').replace('/', '_').replace('\\', '_')

def main():
    """
    Основная функция скрипта.
    """
    global monitoring
    local_config = initialize_config()  # Инициализация конфигурации
    monitoring = MonitoringHandler(config)

    # Проверка существования скрипта мониторинга
    if not os.path.exists(monitoring.script):
        log_manager.logger.error(f"Monitoring script not found at path: {monitoring.script}")
        sys.exit(1)

    # Логирование начала работы
    log_manager.log_lentochka_info("Начало выполнения основного скрипта.")

    # Логика обработки станз
    stanzas = find_stanzas(config)
    successful_copies = 0
    failed_copies = 0
    skipped_copies = 0

    for stanza in stanzas:
        if stanza['status'] == 'failed':
            failed_copies += 1
        elif stanza['status'] != 'not completed':
            successful_copies += 1
        else:
            skipped_copies += 1

    log_manager.logger.info(f"Results: Found {len(stanzas)} rsync.status files, successfully copied: {successful_copies}, skipped: {skipped_copies}, errors: {failed_copies}")

    for stanza in stanzas:
        rsync_status_path = os.path.join(stanza['repo_path'], 'rsync.status')

        # Логирование наличия rsync.status файла
        if os.path.exists(rsync_status_path):
            if os.path.exists(os.path.join(stanza['repo_path'], 'lentochka-status')):
                log_manager.logger.info(f"Stanza ({stanza['repo_path']}) already processed, skipping.")
            else:
                log_manager.logger.info(f"Processing stanza: {stanza['repo_path']}...")
                if process_stanza(stanza, config, monitoring):
                    successful_copies += 1
                else:
                    failed_copies += 1
        else:
            log_manager.logger.info(f"No rsync.status file found at path: {rsync_status_path}")

    # Логирование завершения работы
    log_manager.log_lentochka_info("Script has completed its job and now it's going home")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'Error: {e}')