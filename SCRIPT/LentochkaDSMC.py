#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Optional, List, Dict, Any
from pathlib import Path
import os
import sys
import signal
import logging
import subprocess
import configparser
import time
import psutil
import shutil
import datetime

monitoring = None

class DsmcPlusLentochkaLogs:
    def __init__(self, config_file: Optional[str] = None):
        try:
            self.log_manager = logging.getLogger('log_manager')

            self.config_file = config_file or self.find_config_file()
            print(f"Using config file: {self.config_file}")

            self.config = self.load_config(self.config_file)

            self.search_root = self.config.get('Paths', 'search_root', fallback='')
            self.lentochka_status_dir = self.config.get('Paths', 'lentochka_status_dir', fallback='')
            self.dsmc_log_dir = self.config.get('Logging', 'dsmc_log_dir', fallback='logs/dsmc')
            self.lentochka_log_dir = self.config.get('Logging', 'lentochka_log_dir', fallback='logs/lentochka')
            self.dsmc_command_template = self.config.get('DSMC', 'dsmc_command_template', fallback='dsmc incr {path}')
            self.log_file = self.config.get('Logging', 'log_file', fallback='lentochka.log')
            self.script = self.config.get('Monitoring', 'monitoring_script', fallback=None)

            if not self.search_root:
                self.log_manager.error("ERROR: 'search_root' parameter is missing or empty in the configuration.")
                raise ValueError("'search_root' must be specified in the configuration file.")

            self._ensure_log_directories()

            log_level = self.config.get('Logging', 'log_level', fallback='INFO').upper()
            log_level = getattr(logging, log_level, logging.INFO)
            self.log_manager.setLevel(log_level)

            self._setup_lentochka_logger()
            self._setup_dsmc_logger()

            self.log_manager.info("DsmcPlusLentochkaLogs initialized successfully")
            self.lentochka_logger.info("Lentochka logging system initialized")
            self.dsmc_logger.info("DSMC logging system initialized")

        except Exception as exception:
            print(f"Error during initialization: {exception}")
            raise

    def _ensure_log_directories(self):
        for directory in [self.lentochka_log_dir, self.dsmc_log_dir, self.lentochka_status_dir]:
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
                print(f"Created directory: {directory}")

    @staticmethod
    def find_config_file() -> str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(current_dir, 'LentochkaDSMC.ini')
        if os.path.exists(config_file):
            return config_file

        possible_paths = [
            os.path.join(os.path.expanduser('~'), 'LentochkaDSMC.ini'),
            '/etc/LentochkaDSMC.ini'
        ]

        for path in possible_paths:
            if os.path.exists(path):
                return path

        raise FileNotFoundError("Configuration file not found. Please create LentochkaDSMC.ini in script directory.")

    @staticmethod
    def load_config(config_file: str) -> configparser.ConfigParser:
        if not os.path.exists(config_file):
            error_msg = f"Configuration file not found: {config_file}"
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)

        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def check_write_access(self, directory: str) -> bool:
        if not os.access(directory, os.W_OK):
            self.log_manager.error(f"No write access to directory: {directory}")
            return False
        return True

    def _setup_lentochka_logger(self):
        if not os.path.exists(self.lentochka_log_dir):
            os.makedirs(self.lentochka_log_dir)

        self.lentochka_logger = logging.getLogger('lentochka')
        self.lentochka_logger.handlers = []
        self.lentochka_log_file = os.path.join(self.lentochka_log_dir, 'global-lentochka.log')

        handler = logging.FileHandler(self.lentochka_log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        self.lentochka_logger.addHandler(handler)
        self.lentochka_logger.setLevel(logging.DEBUG)
        self.log_manager.info(f"Logging for Lentochka initialized in file: {self.lentochka_log_file}")

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.current_iteration_log_file = os.path.join(self.lentochka_log_dir, f'lentochka-log-{timestamp}.log')
        self.iteration_handler = logging.FileHandler(self.current_iteration_log_file)
        self.iteration_handler.setFormatter(formatter)
        self.lentochka_logger.addHandler(self.iteration_handler)
        self.log_manager.info(f"Iteration log for Lentochka created at: {self.current_iteration_log_file}")

    def _setup_dsmc_logger(self):
        if not os.path.exists(self.dsmc_log_dir):
            os.makedirs(self.dsmc_log_dir)

        self.dsmc_logger = logging.getLogger('dsmc')
        self.dsmc_logger.handlers = []
        self.dsmc_log_file = os.path.join(self.dsmc_log_dir, 'global-dsmc.log')

        handler = logging.FileHandler(self.dsmc_log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        self.dsmc_logger.addHandler(handler)
        self.dsmc_logger.setLevel(logging.DEBUG)
        self.log_manager.info(f"Logging for DSMC initialized in file: {self.dsmc_log_file}")

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.current_dsmc_session_log_file = os.path.join(self.dsmc_log_dir, f'dsmc-session-{timestamp}.log')
        session_handler = logging.FileHandler(self.current_dsmc_session_log_file)
        session_handler.setFormatter(formatter)
        self.dsmc_logger.addHandler(session_handler)
        self.dsmc_session_handler = session_handler
        self.log_manager.info(f"Session log for DSMC created at: {self.current_dsmc_session_log_file}")

    def log_lentochka_info(self, message):
        self.lentochka_logger.info(message)
        self.log_manager.info(f"[Lentochka] {message}")

    def log_lentochka_error(self, message):
        self.lentochka_logger.error(message)
        self.log_manager.error(f"[Lentochka] {message}")

    def log_dsmc_info(self, message):
        self.dsmc_logger.info(message)
        self.log_manager.info(f"[DSMC] {message}")

    def log_dsmc_error(self, message):
        self.dsmc_logger.error(message)
        self.log_manager.error(f"[DSMC] {message}")

    def append_dsmc_log_to_global(self, log_file_path):
        try:
            with open(log_file_path, 'r') as log_file:
                log_content = log_file.read()
                self.log_dsmc_info(f"--- Begin DSMC log {os.path.basename(log_file_path)} ---")
                for line in log_content.splitlines():
                    self.dsmc_logger.info(line)
                self.log_dsmc_info(f"--- End DSMC log {os.path.basename(log_file_path)} ---")

                self.log_manager.info(f"DSMC log appended to global log: {log_file_path}")
                return True
        except Exception as e:
            self.log_manager.error(f"Error reading DSMC log file: {e}")
            return False

    def close_iteration_log(self):
        if hasattr(self, 'iteration_handler') and self.iteration_handler:
            try:
                self.iteration_handler.close()
                self.lentochka_logger.removeHandler(self.iteration_handler)

                # Проверяем, что файл существует и не пустой
                if os.path.exists(self.current_iteration_log_file) and os.path.getsize(
                        self.current_iteration_log_file) > 0:
                    with open(self.current_iteration_log_file, 'r') as temp_log:
                        log_content = temp_log.read()

                        # Проверяем, что контент еще не был добавлен
                        with open(self.lentochka_log_file, 'r') as check_log:
                            existing_content = check_log.read()
                            if log_content not in existing_content:
                                with open(self.lentochka_log_file, 'a') as global_log:
                                    global_log.write(
                                        f"\n--- Begin Iteration Log {os.path.basename(self.current_iteration_log_file)} ---\n")
                                    global_log.write(log_content)
                                    global_log.write(
                                        f"\n--- End Iteration Log {os.path.basename(self.current_iteration_log_file)} ---\n")

                self.log_manager.info(
                    f"Iteration log closed and appended to global log: {self.current_iteration_log_file}")
            except Exception as e:
                self.log_manager.error(f"Error closing iteration log: {e}")

    def validate_dsmc_log_dir(self):
        if not os.path.exists(self.dsmc_log_dir):
            os.makedirs(self.dsmc_log_dir)
            self.log_manager.info(f"Created DSMC log directory: {self.dsmc_log_dir}")
        return True

    def validate_lentochka_log_dir(self):
        if not os.path.exists(self.lentochka_log_dir):
            os.makedirs(self.lentochka_log_dir)
            self.log_manager.info(f"Created Lentochka log directory: {self.lentochka_log_dir}")
        return True

    def cleanup_empty_logs(self):
        # Если lentochka_status_dir не задан в конфигурации, просто выходим
        if not self.lentochka_status_dir:
            return

        try:
            # Проверяем существование директории перед операциями
            if not os.path.exists(self.lentochka_status_dir):
                return  # Просто выходим без логирования предупреждения

            for log_file in os.listdir(self.lentochka_status_dir):
                if log_file.endswith('.log'):
                    log_path = os.path.join(self.lentochka_status_dir, log_file)
                    if os.path.getsize(log_path) == 0:
                        os.remove(log_path)
                        self.log_manager.info(f'Deleted empty log file: {log_path}')

        except Exception as e:
            # Логируем любые ошибки, но без предупреждения о несуществующей директории
            self.log_manager.error(f"Error during log cleanup: {e}")

class MonitoringHandler:
    def __init__(self, config, log_manager):
        self.config = config
        self.log_manager = log_manager
        self.enabled = config.getboolean('Monitoring', 'enabled', fallback=False)
        self.script = config.get('Monitoring', 'monitoring_script', fallback=None)
        self.interval = config.getint('Monitoring', 'interval', fallback=300)

        self.log_dir = config.get('Paths', 'log_dir', fallback='logs')
        self.log_retention_days = config.getint('Logging', 'log_retention_days', fallback=90)
        self.log_cleanup_enabled = config.getboolean('Logging', 'log_cleanup_enabled', fallback=True)

        search_root = config.get('Paths', 'search_root')
        self.log_manager.info(f"Search directory specified in .ini file: {search_root}")

    @staticmethod
    def sanitize_metric_name(name):
        return name.replace(' ', '_').replace('/', '_').replace('\\', '_')

    def send_metric(self, metric_name, value, status='OK'):
        if not self.enabled or not self.script:
            self.log_manager.warning("Monitoring is disabled or monitoring script is not set.")
            return

        try:
            sanitized_name = self.sanitize_metric_name(metric_name)
            cmd = [self.script, sanitized_name, str(value), status]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.log_manager.info(f"Metric sent: {sanitized_name} with value: {value} and status: {status}")
        except subprocess.CalledProcessError as error:
            self.log_manager.error(f"Error sending metric to monitoring: {error}")

    def cleanup_logs(self, log_dir, log_retention_days):
        if not self.log_cleanup_enabled:
            self.log_manager.info("Automatic log cleanup is disabled.")
            return 0

        # Используем абсолютный путь или текущую директорию, если путь относительный
        if not os.path.isabs(log_dir):
            log_dir = os.path.abspath(log_dir)

        if not os.path.isdir(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
                self.log_manager.info(f"Created log directory: {log_dir}")
            except Exception as e:
                self.log_manager.warning(f"Cannot create log directory {log_dir}: {e}")
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
                    except Exception as file_error:
                        self.log_manager.error(f"Error removing file {log_file_path}: {file_error}")

        if deleted_files_count > 0:
            self.log_manager.info(f"Deleted {deleted_files_count} old logs.")

        return deleted_files_count

    def log_error_with_metrics(self, message, error):
        self.log_manager.error(f"{message}: {error}")
        try:
            if self.enabled and self.script:
                self.send_metric("error", 1, "ERROR")
        except Exception as send_error:
            self.log_manager.error(f"Error sending metrics: {send_error}")

class ProcessLocker:
    def __init__(self, lock_file_path, log_manager, max_instances):
        self.lock_file_path = lock_file_path
        self.pid_file = None
        self.log_manager = log_manager
        self.max_instances = max_instances

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
        except (IOError, ValueError) as read_error:
            self.log_manager.error(f"Error reading PID from file {self.lock_file_path}: {read_error}")
            return None

    def is_process_running(self):
        current_pid = os.getpid()
        running_processes = 0
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['pid'] != current_pid and proc.info['name'] == 'python3':
                cmdline = proc.cmdline()
                if len(cmdline) > 0 and 'LentochkaDSMC.py' in cmdline:
                    running_processes += 1
        if running_processes >= self.max_instances:
            self.log_manager.warning(f"Max instances reached ({self.max_instances}). Process cannot be started.")
            return True
        return False

    def terminate_existing_process(self):
        pid = self._find_existing_process()
        if pid is not None:
            self.log_manager.warning(f"Found active process with PID {pid}. Terminating.")
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(3)
                try:
                    os.kill(pid, signal.SIGKILL)
                    self.log_manager.warning(f"Process {pid} did not terminate, forcing termination.")
                except ProcessLookupError:
                    self.log_manager.info(f"Process {pid} already terminated.")
                if os.path.exists(self.lock_file_path):
                    os.unlink(self.lock_file_path)
            except Exception as terminate_error:
                self.log_manager.error(f"Error terminating process {pid}: {terminate_error}")
        else:
            self.log_manager.info("No active processes to terminate.")

    def is_stale_lock(self):
        if os.path.exists(self.lock_file_path):
            try:
                with open(self.lock_file_path, 'r') as f:
                    pid = int(f.read().strip())
                try:
                    os.kill(pid, 0)
                    return False
                except OSError:
                    return True
            except (IOError, ValueError):
                return True
        return False

    def __enter__(self):
        if self.is_stale_lock():
            self.log_manager.warning("Stale lock file found, removing it.")
            try:
                os.remove(self.lock_file_path)
            except Exception as remove_error:
                self.log_manager.error(f"Error removing stale lock file: {remove_error}")

        if self.is_process_running():
            raise RuntimeError("Another instance of the process is already running.")

        self.terminate_existing_process()
        lock_file = self.lock_file_path

        # Убедимся, что директория существует
        lock_dir = os.path.dirname(lock_file)
        if lock_dir and not os.path.exists(lock_dir):
            os.makedirs(lock_dir)

        if os.path.exists(lock_file):
            os.remove(lock_file)
        self.pid_file = open(lock_file, 'w')
        self.pid_file.write(str(os.getpid()))
        self.pid_file.flush()
        self.log_manager.info(f"Process lock acquired with PID {os.getpid()}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.pid_file:
                self.pid_file.close()
            if os.path.exists(self.lock_file_path):
                os.unlink(self.lock_file_path)
            self.log_manager.info("Process lock released.")
        except Exception as release_error:
            self.log_manager.error(f"Error releasing resources: {release_error}")

class StanzaProcessor:
    def __init__(self, config, lentochka_log):
        self.config = config
        self.lentochka_log = lentochka_log
        self.log_manager = lentochka_log.log_manager

        # Проверяем и создаем необходимые директории для логов
        self.lentochka_log.validate_dsmc_log_dir()
        self.lentochka_log.validate_lentochka_log_dir()

    def find_stanzas(self) -> List[Dict[str, Any]]:
        stanzas = []
        search_root = Path(self.config.get('Paths', 'search_root'))

        rsync_status_count = {'total': 0, 'completed': 0, 'failed': 0, 'missing': 0}
        lentochka_status_count = {'total': 0, 'processed': 0, 'skipped': 0, 'errors': 0}
        repo_count = 0

        def process_directory(current_path):
            nonlocal repo_count

            for repo_dir in current_path.glob('*.repo'):
                repo_count += 1
                backup_dir = repo_dir / 'backup'

                if not backup_dir.exists():
                    self.lentochka_log.log_lentochka_error(f"Backup directory not found: {backup_dir}")
                    continue

                # Проверяем наличие lentochka-status файла
                lentochka_status_path = repo_dir / 'lentochka-status'
                if lentochka_status_path.exists():
                    lentochka_status_count['total'] += 1
                    lentochka_status_count['processed'] += 1
                    self.lentochka_log.log_lentochka_info(f"Stanza already processed: {repo_dir}")
                    continue

                # Recursively find rsync.status files within .repo directory
                rsync_status_files = list(repo_dir.rglob('rsync.status'))

                if not rsync_status_files:
                    self.lentochka_log.log_lentochka_info(f"No rsync.status file found for: {repo_dir}")
                    lentochka_status_count['skipped'] += 1
                    continue

                for rsync_status_path in rsync_status_files:
                    rsync_status_count['total'] += 1

                    try:
                        with open(rsync_status_path, 'r') as f:
                            status_content = f.read().strip().lower()

                            if 'failed' in status_content:
                                rsync_status_count['failed'] += 1
                                self.lentochka_log.log_lentochka_info(
                                    f"Stanza marked as failed: {repo_dir}")
                                continue  # Пропускаем станзы со статусом failed

                            if 'complete' in status_content:
                                status = 'completed'
                                rsync_status_count['completed'] += 1

                                stanza = {
                                    'status_path': str(rsync_status_path),
                                    'repo_path': str(repo_dir),
                                    'backup_path': str(repo_dir),
                                    'status': status,
                                    'subdirs': [d.name for d in backup_dir.iterdir() if d.is_dir()]
                                }

                                stanzas.append(stanza)
                                self.lentochka_log.log_lentochka_info(f"Stanza added to processing queue: {repo_dir}")
                            else:
                                status = 'not completed'
                                rsync_status_count['missing'] += 1
                                self.lentochka_log.log_lentochka_info(
                                    f"Stanza not marked as complete: {repo_dir}")

                    except IOError as exception:
                        self.lentochka_log.log_lentochka_error(
                            f"Error reading file {rsync_status_path}: {exception}")

        process_directory(search_root)

        # Вычисляем skipped для lentochka_status
        lentochka_status_count['skipped'] = repo_count - lentochka_status_count['processed'] - lentochka_status_count[
            'errors']

        result_message = (
            f"RESULTS: "
            f"Found {rsync_status_count['total']} rsync.status files, "
            f"successfully copied: {rsync_status_count['completed']}, "
            f"failed: {rsync_status_count['failed']}, "
            f"missing: {rsync_status_count['missing']}\n"
            f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - RESULTS: Found {lentochka_status_count['total']} lentochka.status files, "
            f"processed: {lentochka_status_count['processed']}, "
            f"skipped: {lentochka_status_count['skipped']}, "
            f"errors: {lentochka_status_count['errors']}"
        )
        self.lentochka_log.log_lentochka_info(result_message)
        return stanzas

    def process_stanza(self, stanza_info: Dict[str, Any]) -> bool:
        """Обрабатывает stanza и запускает команду DSMC"""
        try:
            self.lentochka_log.validate_dsmc_log_dir()

            start_time = datetime.datetime.now()
            self.lentochka_log.log_lentochka_info(
                f"Starting to process stanza: {stanza_info['repo_path']} at {start_time}")

            # Используем backup_path для проверки
            backup_path = Path(stanza_info['backup_path'])
            repo_path = Path(stanza_info['repo_path'])
            lentochka_status_path = repo_path / 'lentochka-status'

            if lentochka_status_path.exists():
                self.lentochka_log.log_lentochka_info(
                    f"Stanza ({stanza_info['repo_path']}) already processed, skipping.")
                return True

            if not repo_path.exists():
                self.lentochka_log.log_lentochka_error(
                    f"Skipping stanza: Path does not exist: {repo_path}")
                return False

            # Проверим статус станзы - не должно быть 'failed'
            if stanza_info.get('status') == 'failed':
                self.lentochka_log.log_lentochka_info(
                    f"Skipping stanza with failed status: {stanza_info['repo_path']}")
                return False

            # Формируем команду DSMC
            dsmc_path = self.config.get('DSMC', 'dsmc_path', fallback='dsmc')
            dsmc_command_template = self.config.get('DSMC', 'dsmc_command_template',
                                                    fallback='{dsmc_path} incr {backup_dirs} -su=yes')

            # Подставляем пути в команду - ИСПОЛЬЗУЕМ backup_path
            command = dsmc_command_template.format(
                dsmc_path=dsmc_path,
                backup_dirs=str(backup_path)  # Путь к backup
            )

            # Вызываем метод run_dsmc_command с обновленной командой
            return_code = self.run_dsmc_command(
                {**stanza_info, 'dsmc_command': command},
                start_time
            )

            # Создаем файл lentochka-status ТОЛЬКО если команда успешно выполнилась
            if return_code == 0:
                end_time = datetime.datetime.now()
                status_content = f"Backup written to tape\nStart: {start_time.isoformat()}\nEnd: {end_time.isoformat()}"

                try:
                    with open(lentochka_status_path, 'w') as f:
                        f.write(status_content)

                    self.lentochka_log.log_lentochka_info(
                        f"Finished processing stanza {stanza_info['repo_path']} - status: completed, file lentochka-status created.")
                    return True
                except Exception as write_error:
                    self.lentochka_log.log_lentochka_error(
                        f"Error creating lentochka-status file: {write_error}")
                    return False
            else:
                self.lentochka_log.log_lentochka_error(
                    f"Error processing stanza {stanza_info['repo_path']} - DSMC command failed with code: {return_code}. lentochka-status NOT created.")
                return False

        except Exception as exception:
            self.lentochka_log.log_lentochka_error(f"Uncaught error processing stanza: {exception}")
            return False

    def run_dsmc_command(self, stanza_info: Dict[str, Any], start_time: datetime.datetime) -> int:
        """Runs DSMC command and redirects output to a log file"""
        log_file_path = None  # Инициализируем переменную заранее

        try:
            # Make sure the log directory exists
            dsmc_log_dir = self.config.get('Logging', 'dsmc_log_dir', fallback='logs/dsmc')
            if not os.path.exists(dsmc_log_dir):
                os.makedirs(dsmc_log_dir)
                self.lentochka_log.log_lentochka_info(f"Created DSMC log directory: {dsmc_log_dir}")

            # Create temporary log filename using stanza path
            stanza_path = stanza_info['repo_path']
            stanza_name = stanza_path.replace('/', '-').replace('\\', '-')
            if stanza_name.startswith('-'):
                stanza_name = stanza_name[1:]  # Remove leading dash if present

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            log_filename = f"dsmc-log-{stanza_name}-{timestamp}.log"
            log_file_path = os.path.join(dsmc_log_dir, log_filename)

            # Log information about command execution
            self.lentochka_log.log_lentochka_info(
                f"Starting DSMC command at {start_time} for stanza: {stanza_info['repo_path']}")
            self.lentochka_log.log_lentochka_info(f"DSMC log will be written to: {log_file_path}")

            # Get command template from config and substitute parameters
            dsmc_path = self.config.get('DSMC', 'dsmc_path', fallback='dsmc')
            command_template = self.config.get('DSMC', 'dsmc_command_template',
                                               fallback='{dsmc_path} incr {repo_path} 2>&1')

            # Form command with parameter substitution
            command = command_template.format(
                dsmc_path=dsmc_path,
                repo_path=stanza_info['repo_path']
            )

            self.lentochka_log.log_lentochka_info(f"Executing command: {command}")

            # Создаем временный лог-файл и выполняем команду
            with open(log_file_path, 'w') as log_file:
                try:
                    # Запускаем команду с перенаправлением stderr в stdout
                    process = subprocess.Popen(
                        command,
                        shell=True,
                        stdout=log_file,
                        stderr=subprocess.STDOUT
                    )
                    process.wait()  # Ждем завершения команды

                    return_code = process.returncode

                except (subprocess.SubprocessError, OSError) as e:
                    error_msg = f"Failed to execute DSMC command: {e}"
                    self.lentochka_log.log_lentochka_error(error_msg)
                    log_file.write(f"ERROR: {error_msg}\n")
                    return 1

            # Логируем результат выполнения команды
            if return_code == 0:
                self.lentochka_log.log_lentochka_info(
                    f"DSMC command completed successfully for stanza: {stanza_info['repo_path']}")
            else:
                self.lentochka_log.log_lentochka_error(
                    f"DSMC command failed for stanza: {stanza_info['repo_path']} with return code: {return_code}")

            # Добавляем содержимое временного лога в глобальный лог DSMC
            self.lentochka_log.append_dsmc_log_to_global(log_file_path)

            return return_code

        except Exception as e:
            # Убедимся, что ошибка будет залогирована даже если что-то пошло не так
            error_msg = f"Error executing DSMC command: {e}"
            self.lentochka_log.log_lentochka_error(error_msg)

            # Создаем лог-файл с ошибкой, если его еще нет
            try:
                if log_file_path:  # Проверяем, что переменная существует и не None
                    # Убедимся, что папка существует
                    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

                    with open(log_file_path, 'w') as error_log:
                        error_log.write(f"CRITICAL ERROR: {error_msg}\n")
                        error_log.write(f"Exception occurred at: {datetime.datetime.now().isoformat()}\n")
                        error_log.write(f"Stanza path: {stanza_info['repo_path']}\n")

                    self.lentochka_log.append_dsmc_log_to_global(log_file_path)
                else:
                    # Если log_file_path еще не был создан, создаем новый файл
                    try:
                        dsmc_log_dir = self.config.get('Logging', 'dsmc_log_dir', fallback='logs/dsmc')
                        if not os.path.exists(dsmc_log_dir):
                            os.makedirs(dsmc_log_dir)

                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                        log_filename = f"dsmc-error-{timestamp}.log"
                        error_log_path = os.path.join(dsmc_log_dir, log_filename)

                        with open(error_log_path, 'w') as error_log:
                            error_log.write(f"CRITICAL ERROR: {error_msg}\n")
                            error_log.write(f"Exception occurred at: {datetime.datetime.now().isoformat()}\n")
                            error_log.write(f"Stanza path: {stanza_info['repo_path']}\n")

                        self.lentochka_log.append_dsmc_log_to_global(error_log_path)
                    except Exception as e:
                        self.lentochka_log.log_lentochka_error(f"Failed to create emergency error log: {e}")
            except Exception as log_error:
                self.lentochka_log.log_lentochka_error(f"Failed to create error log file: {log_error}")

            return 1  # Возвращаем код ошибки

    def _check_dsmc_exists(self, dsmc_path: str) -> bool:
        """Проверяет существование команды DSMC"""
        try:
            self.lentochka_log.log_lentochka_info(f"Checking existence of DSMC at path: {dsmc_path}")

            if os.path.isabs(dsmc_path):
                # Если указан абсолютный путь, проверяем существование файла
                exists = os.path.exists(dsmc_path) and os.access(dsmc_path, os.X_OK)
                if exists:
                    self.lentochka_log.log_lentochka_info(f"Found DSMC executable at: {dsmc_path}")
                else:
                    self.lentochka_log.log_lentochka_error(f"DSMC executable not found at path: {dsmc_path}")
                return exists
            else:
                # Если указана команда без пути, проверяем через which/where
                which_cmd = 'where' if sys.platform == 'win32' else 'which'
                try:
                    result = subprocess.run([which_cmd, dsmc_path],
                                            capture_output=True,
                                            text=True)

                    if result.returncode == 0:
                        dsmc_full_path = result.stdout.strip()
                        self.lentochka_log.log_lentochka_info(f"Found DSMC in PATH at: {dsmc_full_path}")
                        return True
                    else:
                        self.lentochka_log.log_lentochka_error(f"DSMC utility not found in PATH")
                        return False
                except Exception as which_error:
                    self.lentochka_log.log_lentochka_error(f"Error checking DSMC with '{which_cmd}': {which_error}")
                    return False
        except Exception as e:
            self.lentochka_log.log_lentochka_error(f"Error checking DSMC existence: {e}")
            return False

def main():
    global monitoring
    dsmc_log = None  # Инициализируем переменную заранее
    monitoring = None  # Инициализируем переменную заранее

    try:
        # Create logging object
        dsmc_log = DsmcPlusLentochkaLogs()

        # Create monitoring object
        monitoring = MonitoringHandler(dsmc_log.config, dsmc_log.log_manager)

        # Get max number of simultaneous instances
        max_instances = dsmc_log.config.getint('Process', 'max_instances', fallback=1)

        # Get path to lock file
        lock_file = dsmc_log.config.get('Paths', 'lock_file', fallback='/tmp/lentochka_dsmc.lock')

        # Create process locker
        process_locker = ProcessLocker(lock_file, dsmc_log.log_manager, max_instances)

        with process_locker:
            # Check monitoring script existence
            if monitoring.script and not os.path.exists(monitoring.script):
                dsmc_log.log_manager.error(f"Monitoring script not found at path: {monitoring.script}")
                monitoring.enabled = False

            dsmc_log.log_manager.info("Starting main script execution.")

            # Создаем объект StanzaProcessor
            stanza_processor = StanzaProcessor(dsmc_log.config, dsmc_log)

            # Find stanzas for processing
            stanzas = stanza_processor.find_stanzas()

            # Проверяем наличие DSMC
            dsmc_path = dsmc_log.config.get('DSMC', 'dsmc_path', fallback='dsmc')
            dsmc_exists = shutil.which(dsmc_path) is not None

            if not dsmc_exists:
                error_msg = "DSMC utility not found. Please specify the correct path to the dsmc utility in LentochkaDSMC.ini"
                dsmc_log.log_manager.error(error_msg)
                if monitoring and monitoring.enabled:
                    try:
                        monitoring.send_metric("dsmc_not_found", 1, "ERROR")
                    except Exception as send_error:
                        dsmc_log.log_manager.error(f"Error sending metric for dsmc not found: {send_error}")
                # Exit the script
                sys.exit(1)

            successful_copies = 0
            failed_copies = 0
            skipped_copies = 0

            # Обрабатываем каждую stanza
            for stanza in stanzas:
                # Проверяем, что станза не имеет статус failed
                if stanza.get('status') == 'failed':
                    dsmc_log.log_manager.info(f"Skipping stanza with failed status: {stanza['repo_path']}")
                    skipped_copies += 1
                    if monitoring.enabled:
                        monitoring.send_metric("skipped_stanzas", 1)
                    continue

                if os.path.exists(os.path.join(stanza['repo_path'], 'lentochka-status')):
                    dsmc_log.log_manager.info(f"Stanza ({stanza['repo_path']}) already processed, skipping.")
                    skipped_copies += 1
                    if monitoring.enabled:
                        monitoring.send_metric("skipped_stanzas", 1)
                else:
                    dsmc_log.log_manager.info(f"Processing stanza: {stanza['repo_path']}...")
                    if stanza_processor.process_stanza(stanza):
                        successful_copies += 1
                        if monitoring.enabled:
                            monitoring.send_metric("processed_stanzas", 1)
                    else:
                        failed_copies += 1
                        if monitoring.enabled:
                            monitoring.send_metric("failed_stanzas", 1)

            dsmc_log.log_manager.info(f"Results: Found {len(stanzas)} stanzas, "
                                      f"successfully processed: {successful_copies}, skipped: {skipped_copies}, errors: {failed_copies}")

            # Очищаем пустые лог-файлы
            dsmc_log.cleanup_empty_logs()

            # Закрываем итерационный лог
            dsmc_log.close_iteration_log()

            # Очищаем старые логи
            if monitoring.log_cleanup_enabled:
                monitoring.cleanup_logs(monitoring.log_dir, monitoring.log_retention_days)

            dsmc_log.log_manager.info("Script has completed successfully")

    except FileNotFoundError as e:
        print(f"File not found: {e}")
        if dsmc_log and hasattr(dsmc_log, 'log_manager'):
            dsmc_log.log_manager.error(f"File not found: {e}")
        if monitoring and monitoring.enabled:
            try:
                monitoring.send_metric("script_error", 1, "ERROR")
            except Exception as send_error:
                if dsmc_log and hasattr(dsmc_log, 'log_manager'):
                    dsmc_log.log_manager.error(f"Error sending metric for FileNotFoundError: {send_error}")
        sys.exit(1)

    except ValueError as e:
        print(f"Value error: {e}")
        if dsmc_log and hasattr(dsmc_log, 'log_manager'):
            dsmc_log.log_manager.error(f"Value error: {e}")
        if monitoring and monitoring.enabled:
            try:
                monitoring.send_metric("script_error", 1, "ERROR")
            except Exception as send_error:
                if dsmc_log and hasattr(dsmc_log, 'log_manager'):
                    dsmc_log.log_manager.error(f"Error sending metric for ValueError: {send_error}")
        sys.exit(1)

    except Exception as e:
        print(f"Unexpected error: {e}")
        if dsmc_log and hasattr(dsmc_log, 'log_manager'):
            dsmc_log.log_manager.error(f"Unexpected error: {e}")
        if monitoring and monitoring.enabled:
            try:
                monitoring.send_metric("script_error", 1, "ERROR")
            except Exception as send_error:
                if dsmc_log and hasattr(dsmc_log, 'log_manager'):
                    dsmc_log.log_manager.error(f"Error sending metric for unexpected error: {send_error}")
        sys.exit(1)


if __name__ == '__main__':
    main()