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

def initialize_config():
    """
    Ищет и загружает конфигурационный файл.
    Returns:
        dict: Словарь с параметрами конфигурации
    """
    global CONFIG_FILE
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))
    logger.info(f"Project path: {project_root}")  # Логирование пути к проекту
    CONFIG_FILE = os.path.join(project_root, "LentochkaDSMC.ini")
    if not os.path.exists(CONFIG_FILE):
        error_msg = f"Configuration file not found! Looking for path: {CONFIG_FILE}"
        raise FileNotFoundError(error_msg)

    # Загружаем конфиг
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    
    # Проверка на наличие файла
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f'Configuration file not found: {CONFIG_FILE}')  
    
    # Устанавливаем дефолтные значения
    config.setdefault('Paths', {})
    config.setdefault('DSMC', {})
    config.setdefault('Logging', {})
    config.setdefault('Monitoring', {})
    
    # Пути
    config['Paths']['config_path'] = config.get('Paths', 'config_path', fallback=CONFIG_FILE)
    config['Paths']['log_dir'] = config.get('Paths', 'log_dir')
    config['Paths']['search_root'] = config.get('Paths', 'search_root', fallback='')
    config['Paths']['excluded_dirs'] = config.get('Paths', 'excluded_dirs', 
                                                  fallback='/proc,/sys,/dev,/run,/tmp,/var/cache,/var/tmp')
    
    # Настройки DSMC
    config['DSMC']['dsmc_path'] = config.get('DSMC', 'dsmc_path', fallback='dsmc')
    config['DSMC']['additional_params'] = config.get('DSMC', 'additional_params', fallback='-quiet')
    config['DSMC']['max_backup_copies'] = config.get('DSMC', 'max_backup_copies', fallback='5')
    
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

                        logger.info(f"Found rsync.status file at the following path: {status_path}")
                        logger.info(f"Status of rsync file: {status}")
                except IOError as e:
                    logger.error(f"Error reading file {status_path}: {e}")
                    continue

                # Добавляем станзу в список
                stanzas.append({
                    'status_path': status_path,
                    'repo_path': root,
                    'status': status
                })

    return stanzas

def process_stanza(stanza_info, config, monitoring):
    dsmc_path = config.get('DSMC', 'dsmc_path', fallback='dsmc')
    try:
        # Установка времени начала
        start_time = datetime.datetime.now()

        # Проверяем наличие файла lentochka-status
        status_dir = os.path.dirname(stanza_info['status_path'])
        lentochka_status_path = os.path.join(status_dir, 'lentochka-status')
        if os.path.exists(lentochka_status_path):
            logger.info(f"Stanza ({stanza_info['repo_path']}) already processed, skipping.")
            return True

        # Проверяем содержимое rsync.status
        with open(stanza_info['status_path'], 'r') as f:
            rsync_status = f.read().strip().lower()
        if not rsync_status.startswith("complete"):
            logger.warning(f"Skipping stanza ({stanza_info['repo_path']}): rsync status does not start with 'complete' (found: '{rsync_status}').")
            return False

        # Обрабатываем только саму директорию .repo
        repo_path = stanza_info['repo_path']
        if not os.path.exists(repo_path):
            logger.error(f"Skipping stanza ({repo_path}): directory does not exist.")
            return False

        dsmc_logger.info(f"Backing up contents of directory: {repo_path}")
        command = f'{dsmc_path} incr "{repo_path}" -su=yes'  # Копируем всю директорию и её подкаталоги
        try:
            result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            dsmc_logger.info(f"Command dsmc executed successfully: {result.stdout.decode()}")
        except subprocess.CalledProcessError as e:
            dsmc_logger.error(f"Error executing command dsmc: {e.stderr.decode()}")
            dsmc_logger.error(f"Failed command: {command}")
            return False

        # Создаем lentochka-status
        end_time = datetime.datetime.now()
        status_content = f"Backup written to tape\nStart: {start_time.isoformat()}\nEnd: {end_time.isoformat()}"
        with open(lentochka_status_path, 'w') as f:
            f.write(status_content)

        dsmc_logger.info(f"Finished processing stanza {stanza_info['repo_path']} - status: {stanza_info['status']}, file lentochka-status created.")
        return True

    except Exception as e:
        logger.error(f"Uncaught error processing stanza: {e}")
        return False

def generate_dsmc_log_path(log_dir):
    """
    Генерирует путь к логу DSMC с таймштампом.
    """
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file_name = f'lentochka_{timestamp}.log'
    return Path(log_dir) / log_file_name

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
    if not os.path.exists(monitoring.script):
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

    logger.info(f"Results: Found {len(stanzas)} rsync.status files, successfully copied: {successful_copies}, skipped: {skipped_copies}, errors: {failed_copies}")

    for stanza in stanzas:
        rsync_status_path = os.path.join(stanza['repo_path'], 'rsync.status')

        # Логирование наличия rsync.status файла
        if os.path.exists(rsync_status_path):
            if os.path.exists(os.path.join(stanza['repo_path'], 'lentochka-status')):
                logger.info(f"Stanza ({stanza['repo_path']}) already processed, skipping.")
            else:
                logger.info(f"Processing stanza: {stanza['repo_path']}...")
                if process_stanza(stanza, config, monitoring):
                    successful_copies += 1
                else:
                    failed_copies += 1
        else:
            logger.info(f"No rsync.status file found at path: {rsync_status_path}")

    # Завершение работы логгера DSMC
    for handler in dsmc_logger.handlers[:]:
        handler.close()
        dsmc_logger.removeHandler(handler)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'Error: {e}')