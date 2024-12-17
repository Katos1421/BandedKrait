#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Подключаем необходимые библиотеки
import os
import sys
import fcntl
import signal
import logging
import subprocess
import configparser
import time
import shutil
import psutil
import datetime
from pathlib import Path

# Глобальная инициализация логгера
logger = logging.getLogger()
dsmc_logger = None

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
        logger.info(f"Search directory specified in .ini file: {search_root}")
        
    def send_metric(self, metric_name, value, status='OK'):
        """Отправка метрики в систему мониторинга."""
        if not self.enabled or not self.script:
            logger.warning("Monitoring is disabled or monitoring script is not set.")
            return
            
        try:
            cmd = [self.script, metric_name, str(value), status]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info(f"Metric sent: {metric_name} with value: {value} and status: {status}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error sending metric to monitoring: {e}")
    
    def cleanup_logs(self, log_dir, log_retention_days):
        """
        Очистка старых логов.
        """
        if not self.log_cleanup_enabled:
            logging.info("Automatic log cleanup is disabled.")
            return 0
        if not os.path.isdir(log_dir):
            logger.warning(f"Log directory does not exist: {log_dir}")
            return 0
        deleted_files_count = 0
        for log_file in os.listdir(log_dir):
            log_file_path = os.path.join(log_dir, log_file)
            if os.path.isfile(log_file_path):
                file_age_days = (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(log_file_path))).days
                if file_age_days > log_retention_days:
                    try:
                        os.remove(log_file_path)
                        deleted_files_count += 1
                    except Exception as e:
                        logger.error(f"Error removing file {log_file_path}: {e}")
        if deleted_files_count > 0:
            logger.info(f"Deleted {deleted_files_count} old logs.")
            logger.info(f"Successfully deleted old logs.")
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
            logger.error(f"Error reading PID from file {self.lock_file_path}: {e}")
            return None
    
    def terminate_existing_process(self):
        pid = self._find_existing_process()
        if pid is not None:
            logger.warning(f"Found active process with PID {pid}. Terminating.")
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(3)
                try:
                    os.kill(pid, signal.SIGKILL)
                    logger.warning(f"Process {pid} did not terminate, forcing termination.")
                except ProcessLookupError:
                    logger.info(f"Process {pid} already terminated.")
                if os.path.exists(self.lock_file_path):
                    os.unlink(self.lock_file_path)
            except Exception as e:
                logger.error(f"Error terminating process {pid}: {e}")
        else:
            logger.info("No active processes to terminate.")
    
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
            logger.error(f"Error releasing resources: {e}")

class FileProcessingStats:
    def __init__(self):
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.in_progress = 0
        self.not_found = 0
        self.completed_copies = 0  # Успешные копирования
        self.failed_copies = 0     # Неуспешные копирования

    def update(self, status, is_copy=False):
        if status == 'completed':
            self.completed += 1
        elif status == 'failed':
            self.failed += 1
        elif status == 'skipped':
            self.skipped += 1
        elif status == 'in_progress':
            self.in_progress += 1
        elif status == 'not_found':
            self.not_found += 1
        
        # Для копирований
        if is_copy:
            if status == 'completed':
                self.completed_copies += 1
            elif status == 'failed':
                self.failed_copies += 1

    def log_results(self):
        logger.info(f"Results: Completed files: {self.completed} \
                    Failed files: {self.failed} \
                    Skipped files: {self.skipped} \
                    In progress files: {self.in_progress} \
                    Not found files: {self.not_found}")
        # Логирование итогов копирования
        logger.info(f"Copying results: Successful copies: {self.completed_copies} \
                    Failed copies: {self.failed_copies}")

def validate_and_prepare_log_dir(log_dir):
    """
    Проверяет и подготавливает директорию для логов.
    """
    if not log_dir or log_dir.strip() == '':
        error_msg = "ERROR: LOG DIRECTORY NOT SET IN CONFIGURATION FILE!"
        logger.error(error_msg)
        monitoring.send_metric("log_dir_error", 1, "ERROR")
        raise ValueError(error_msg)
    project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    log_dir = os.path.join(project_root, log_dir.strip())
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
        logger.info(f"Created log directory: {log_dir}")
    elif not os.access(log_dir, os.W_OK):
        raise PermissionError(f"No write access to directory: {log_dir}")
    return log_dir

def validate_and_prepare_dsmc_log_dir(dsmc_log_dir):
    """
    Проверка и подготовка директории для логов DSMC.
    """
    if not dsmc_log_dir or dsmc_log_dir.strip() == '':
        error_msg = "ERROR: DSMC LOG DIRECTORY NOT SET IN CONFIGURATION FILE!"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    dsmc_log_dir = os.path.join(project_root, dsmc_log_dir.strip())
    
    if not os.path.exists(dsmc_log_dir):
        try:
            os.makedirs(dsmc_log_dir, exist_ok=True)
            logger.info(f"Created DSMC log directory: {dsmc_log_dir}")
        except Exception as e:
            logger.error(f"Failed to create DSMC log directory {dsmc_log_dir}: {e}")
            raise PermissionError(f"Failed to create DSMC log directory: {dsmc_log_dir}")
    
    elif not os.access(dsmc_log_dir, os.W_OK):
        error_msg = f"ERROR: No write access to DSMC log directory: {dsmc_log_dir}"
        logger.error(error_msg)
        raise PermissionError(error_msg)

    return dsmc_log_dir

def initialize_config():
    """
    Ищет и загружает конфигурационный файл.
    Returns:
        dict: Словарь с параметрами конфигурации
    """
    global CONFIG_FILE
    # Считываем путь к директории с конфигурационным файлом из ini
    config_dir = '.'  # Параметр из конфигурации
    config_path = 'LentochkaDSMC.ini'  # Имя файла конфигурации
    CONFIG_FILE = os.path.join(config_dir, config_path)  # Строим полный путь

    logger.info(f"Configuration file path: {CONFIG_FILE}")
    if not os.path.exists(CONFIG_FILE):
        error_msg = f"Configuration file not found! Looking for path: {CONFIG_FILE}"
        raise FileNotFoundError(error_msg)

    # Загружаем конфиг
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    
    # Устанавливаем дефолтные значения
    config.setdefault('Paths', {})
    config.setdefault('DSMC', {})
    config.setdefault('Logging', {})
    config.setdefault('Monitoring', {})
    
    # Пути
    config['Paths']['log_dir'] = config.get('Paths', 'log_dir')
    config['Paths']['search_root'] = config.get('Paths', 'search_root', fallback='')
    config['Paths']['excluded_dirs'] = config.get('Paths', 'excluded_dirs', 
                                                fallback='/proc,/sys,/dev,/run,/tmp,/var/cache,/var/tmp')
    
    # Настройки DSMC
    config['DSMC']['dsmc_path'] = config.get('DSMC', 'dsmc_path', fallback='dsmc')
    config['DSMC']['additional_params'] = config.get('DSMC', 'additional_params', fallback='-quiet')
    config['DSMC']['max_backup_copies'] = config.get('DSMC', 'max_backup_copies', fallback='5')
    config['DSMC']['dsmc_log_dir'] = config.get('DSMC', 'dsmc_log_dir', fallback=None)
    
    # Настройки логирования
    config['Logging']['level'] = config.get('Logging', 'level', fallback='INFO')
    config['Logging']['message_format'] = config.get('Logging', 'message_format', 
                                                    fallback='%%(asctime)s - %%(levelname)s - %%(message)s',
                                                    raw=True)
    config['Logging']['time_format'] = config.get('Logging', 'time_format', 
                                                fallback='%Y-%m-%d %H:%M:%S',
                                                raw=True)
    config['Logging']['log_retention_days'] = config.get('Logging', 'log_retention_days', fallback='90')
    config['Logging']['log_cleanup_enabled'] = config.get('Logging', 'log_cleanup_enabled', fallback='true')
    
    # Мониторинг
    config['Monitoring']['enabled'] = config.get('Monitoring', 'enabled', fallback='true')
    config['Monitoring']['zabbix_server'] = config.get('Monitoring', 'zabbix_server', fallback='127.0.0.1')
    config['Monitoring']['monitoring_script'] = config.get('Monitoring', 'monitoring_script', 
        fallback='/path/to/monitoring/script.sh')
    config['Monitoring']['interval'] = config.get('Monitoring', 'interval', fallback='300')
    
    log_dir_value = config.get('Paths', 'log_dir')
    if not log_dir_value:
        raise ValueError("Log directory path not set in configuration.")
    log_dir = log_dir_value  # оставляем как строку
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)  # Создание директории, если ее нет
    if not os.access(log_dir, os.W_OK):
        logger.error(f"No write access to directory: {log_dir}")
        raise PermissionError(f"No write access to directory: {log_dir}")
    config['Paths']['log_dir'] = log_dir  # оставляем как строку
    
    return config

def find_stanzas(config, stats):
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

                        logger.info(f"Found rsync.status file at the following path: {status_path}")
                        logger.info(f"Status of rsync file: {status}")
                        
                        # Обновляем статистику
                        stats.update(status)
                        logger.info(f"File {status_path} processed with status: {status}.")

                except IOError as e:
                    logger.error(f"Error reading file {status_path}: {e}")
                    stats.update('failed')
                    continue

                # Добавляем станзу в список
                stanzas.append({
                    'status_path': status_path,
                    'repo_path': root,
                    'status': status
                })

    return stanzas

def process_stanza(stanza_info, config, monitoring, stats):
    dsmc_path = config.get('DSMC', 'dsmc_path', fallback='dsmc')
    dsmc_log_dir = config.get('DSMC', 'dsmc_log_dir', fallback=None)

    # Проверка, если путь к логам DSMC не задан
    if not dsmc_log_dir:
        error_msg = "DSMC log directory is not specified in the config file."
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Подготовка директории логов DSMC
    try:
        dsmc_log_dir = validate_and_prepare_dsmc_log_dir(dsmc_log_dir)
    except Exception as e:
        logger.error(f"Error validating DSMC log directory: {e}")
        raise

    try:
        # Установка времени начала
        start_time = datetime.datetime.now()

        # Проверяем наличие файла lentochka-status
        status_dir = os.path.dirname(stanza_info['status_path'])
        lentochka_status_path = os.path.join(status_dir, 'lentochka-status')
        if os.path.exists(lentochka_status_path):
            logger.info(f"Stanza ({stanza_info['repo_path']}) already processed, skipping.")
            stats.update('skipped')  # Обновляем счетчик пропущенных
            return True

        # Проверяем содержимое rsync.status
        with open(stanza_info['status_path'], 'r') as f:
            rsync_status = f.read().strip().lower()
        if not rsync_status.startswith("complete"):
            stats.update('skipped')  # Обновляем счетчик пропущенных
            return False

        # Обрабатываем только саму директорию .repo
        repo_path = stanza_info['repo_path']
        if not os.path.exists(repo_path):
            logger.error(f"Skipping stanza ({repo_path}): directory does not exist.")
            stats.update('failed')
            return False

        # Проверка наличия папок backup и archive
        backup_path = os.path.join(repo_path, 'backup')
        archive_path = os.path.join(repo_path, 'archive')

        # Проверяем, что папки backup и archive существуют
        if not os.path.exists(backup_path) and not os.path.exists(archive_path):
            logger.warning(f"Skipping stanza ({repo_path}): Neither 'backup' nor 'archive' directories exist.")
            stats.update('failed')
            return False

        # Логирование процесса
        dsmc_logger.info(f"Backing up contents of directories: {backup_path} and {archive_path}")

        # Генерируем уникальное имя для лога для каждой итерации
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file_name = f"dsmc-{os.path.basename(repo_path)}-{timestamp}.log"
        log_file_path = os.path.join(dsmc_log_dir, log_file_name)

        # Формируем команду для копирования содержимого только из backup и archive
        command = f'{dsmc_path} incr "{backup_path}" "{archive_path}" -su=yes >> "{log_file_path}" 2>&1'

        # Записываем команду в lentochka-status с текущим временем
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(lentochka_status_path, 'a') as status_file:  
            status_file.write(f"{current_time} - Running command: {command}\n")

        # Выполняем команду и логируем результат
        try:
            result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(lentochka_status_path, 'a') as status_file:
                status_file.write(f"{current_time} - Command executed successfully (code 0)\n")
            dsmc_logger.info(f"Backup completed for {repo_path} (backup and archive). Log saved to {log_file_path}")
            stats.update('completed', is_copy=True)  # Успешное копирование
            return 0  # Успешное выполнение
        except subprocess.CalledProcessError as e:
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_message = f"Error during backup for {repo_path}. Check log file for details: {log_file_path}. Error: {e.stderr.decode()}"
            with open(lentochka_status_path, 'a') as status_file:
                status_file.write(f"{current_time} - Command failed (code 1): {error_message}\n")
            dsmc_logger.error(f"Command failed with error code {e.returncode}: {error_message}")
            stats.update('failed', is_copy=True)  # Неудачное копирование
            return 1  # Ошибка выполнения команды

        # Создаем lentochka-status
        end_time = datetime.datetime.now()
        status_content = f"Backup written to tape\nStart: {start_time.isoformat()}\nEnd: {end_time.isoformat()}"
        with open(lentochka_status_path, 'a') as f:
            f.write(status_content)

        dsmc_logger.info(f"Finished processing stanza {stanza_info['repo_path']} - status: {stanza_info['status']}, file lentochka-status created.")
        return True

    except Exception as e:
        logger.error(f"Uncaught error processing stanza: {e}")
        stats.update('failed')  # Обновляем статистику на случай ошибки
        return False

    # Логируем итоговые результаты
    stats.log_results()

def generate_dsmc_log_path(log_dir):
    """
    Генерирует путь к логу DSMC с таймштампом.
    """
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file_name = f'lentochka_{timestamp}.log'
    log_file_path = Path(log_dir) / log_file_name

    # Экранируем путь к лог-файлу
    return str(log_file_path)

def log_error_with_metrics(message, error):
    logger.error(f"{message}: {error}")
    try:
        if monitoring:
            monitoring.send_metric("error", 1, "ERROR")
    except Exception as e:
        logger.error(f"Error sending metrics: {e}")

def check_write_access(directory):
    if not os.access(directory, os.W_OK):
        logger.error(f"No write access to directory: {directory}")
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
    global monitoring, dsmc_logger
    config = initialize_config()  # Инициализация конфигурации
    monitoring = MonitoringHandler(config)

    # Проверка существования скрипта мониторинга
    if monitoring.enabled and monitoring.script and not os.path.exists(monitoring.script):
        logger.error(f"Monitoring script not found at path: {monitoring.script}")
        sys.exit(1)

    # Инициализация логгера DSMC
    dsmc_log_file_path = os.path.join(
        config.get('Paths', 'log_dir'),
        f'dsmc-log-{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    )
    dsmc_logger = logging.getLogger('dsmc')
    dsmc_logger.setLevel(logging.INFO)
    dsmc_file_handler = logging.FileHandler(dsmc_log_file_path, mode='a')
    dsmc_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    dsmc_logger.addHandler(dsmc_file_handler)
    dsmc_logger.info(f"Initializing DSMC logger. Logs will be written to file: {dsmc_log_file_path}")

    # Настройка логгера
    log_level = getattr(logging, config.get('Logging', 'level', fallback='INFO').upper(), logging.INFO)
    log_format = config.get('Logging', 'message_format', fallback='%%(asctime)s - %%(levelname)s - %%(message)s')
    time_format = config.get('Logging', 'time_format', fallback='%Y-%m-%d %H:%M:%S')
    log_file_path = os.path.join(config.get('Paths', 'log_dir'), f'lentochka-log-{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logger.info(f"Log file path: {log_file_path}")

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=time_format,
        filename=log_file_path,
        filemode='a'
    )

    # Добавляем обработчик для консоли
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format, datefmt=time_format))
    logger.addHandler(console_handler)

    # Создаем объект для статистики
    stats = FileProcessingStats()

    # Логика обработки станз
    stanzas = find_stanzas(config, stats)
    
    for stanza in stanzas:
        rsync_status_path = os.path.join(stanza['repo_path'], 'rsync.status')

        # Логирование наличия rsync.status файла
        if os.path.exists(rsync_status_path):
            if os.path.exists(os.path.join(stanza['repo_path'], 'lentochka-status')):
                logger.info(f"Stanza ({stanza['repo_path']}) already processed, skipping.")
                stats.update('skipped')  # Обновляем счетчик пропущенных
            else:
                logger.info(f"Processing stanza: {stanza['repo_path']}...")
                if process_stanza(stanza, config, monitoring, stats):
                    stats.update('completed')
                else:
                    stats.update('failed')
        else:
            logger.info(f"No rsync.status file found at path: {rsync_status_path}")
            stats.update('not_found')

    # Логируем итоговые результаты
    stats.log_results()

    # Завершение работы логгера DSMC
    for handler in dsmc_logger.handlers[:]:
        handler.close()
        dsmc_logger.removeHandler(handler)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'Error: {e}')
