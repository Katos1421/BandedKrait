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
    def __init__(self, config_file=None):
        try:
            # Установим сначала базовое логирование для отладки
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
            self.log_manager = logging.getLogger('log_manager')

            # Найдем файл конфигурации
            self.config_file = config_file or self._find_config_file()
            print(f"Using config file: {self.config_file}")

            # Загрузим конфигурацию
            self.config = self.load_config(self.config_file)

            # Теперь настроим остальные параметры
            self.search_root = self.config.get('Paths', 'search_root', fallback='')
            self.lentochka_status_dir = self.config.get('Paths', 'lentochka_status_dir', fallback='')
            self.dsmc_command_template = self.config.get('DSMC', 'dsmc_command_template', fallback='')
            self.log_file = self.config.get('Logging', 'log_file', fallback='lentochka.log')
            self.script = self.config.get('Monitoring', 'monitoring_script', fallback=None)

            if not self.search_root:
                self.log_manager.error("ERROR: 'search_root' parameter is missing or empty in the configuration.")
                raise ValueError("'search_root' must be specified in the configuration file.")

            log_level = self.config.get('Logging', 'log_level', fallback='INFO').upper()
            log_level = getattr(logging, log_level, logging.INFO)
            self.log_manager.setLevel(log_level)

            self._setup_lentochka_logger()
            self._setup_dsmc_logger()

        except Exception as exception:
            print(f"Error during initialization: {exception}")
            raise

    def _find_config_file(self):
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
    def load_config(config_file):
        if not os.path.exists(config_file):
            error_msg = f"Configuration file not found: {config_file}"
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)

        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def check_write_access(self, directory):
        if not os.access(directory, os.W_OK):
            self.log_manager.error(f"No write access to directory: {directory}")
            return False
        return True

    def _setup_lentochka_logger(self):
        lentochka_log_dir = self.config.get('Logging', 'lentochka_log_dir', fallback='')

        if lentochka_log_dir and not os.path.exists(lentochka_log_dir):
            os.makedirs(lentochka_log_dir)

        self.lentochka_logger = logging.getLogger('lentochka')
        self.lentochka_log_file = os.path.join(lentochka_log_dir, 'global-lentochka.log')

        handler = logging.FileHandler(self.lentochka_log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        self.lentochka_logger.addHandler(handler)
        self.lentochka_logger.setLevel(logging.DEBUG)
        self.lentochka_logger.info(f"Logging for Lentochka initialized in file: {self.lentochka_log_file}")

    def _setup_dsmc_logger(self):
        dsmc_log_dir = self.config.get('Logging', 'dsmc_log_dir', fallback='')

        if dsmc_log_dir and not os.path.exists(dsmc_log_dir):
            os.makedirs(dsmc_log_dir)

        self.dsmc_logger = logging.getLogger('dsmc')
        self.dsmc_log_file = os.path.join(dsmc_log_dir, 'global-dsmc.log')

        handler = logging.FileHandler(self.dsmc_log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        self.dsmc_logger.addHandler(handler)
        self.dsmc_logger.setLevel(logging.DEBUG)
        self.dsmc_logger.info(f"Logging for DSMC initialized in file: {self.dsmc_log_file}")

    def log_lentochka_info(self, message):
        self.lentochka_logger.info(message)

    def log_lentochka_error(self, message):
        self.lentochka_logger.error(message)

    def validate_dsmc_log_dir(self):
        dsmc_log_dir = self.config.get('Logging', 'dsmc_log_dir', fallback='')
        if dsmc_log_dir and not os.path.exists(dsmc_log_dir):
            os.makedirs(dsmc_log_dir)
        return True

    def cleanup_empty_logs(self):
        if not os.path.exists(self.lentochka_status_dir):
            self.log_manager.warning(f"Lentochka status directory does not exist: {self.lentochka_status_dir}")
            return

        for log_file in os.listdir(self.lentochka_status_dir):
            if log_file.endswith('.log'):
                log_path = os.path.join(self.lentochka_status_dir, log_file)
                if os.path.getsize(log_path) == 0:
                    os.remove(log_path)
                    self.log_manager.info(f'Deleted empty log file: {log_path}')

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
        if not os.path.isdir(log_dir):
            self.log_manager.warning(f"Log directory does not exist: {log_dir}")
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

    def find_stanzas(self):
        stanzas = []

        rsync_status_count = {'completed': 0, 'failed': 0, 'missing': 0}
        lentochka_status_count = {'processed': 0, 'skipped': 0}

        search_root = self.config.get('Paths', 'search_root')
        if not os.path.exists(search_root):
            self.log_manager.error(f"Search root directory does not exist: {search_root}")
            return stanzas

        for root, dirs, files in os.walk(search_root):
            for file in files:
                if file == 'rsync.status':
                    status_path = os.path.join(root, file)

                    try:
                        with open(status_path, 'r') as f:
                            status_content = f.read().strip().lower()

                            if 'complete' in status_content:
                                status = 'completed'
                                rsync_status_count['completed'] += 1
                            elif 'failed' in status_content:
                                status = 'failed'
                                rsync_status_count['failed'] += 1
                            else:
                                status = 'not completed'
                                rsync_status_count['missing'] += 1

                            self.log_manager.info(f"Found rsync.status file: {status_path}")
                            self.log_manager.info(f"rsync status: {status}")

                    except IOError as exception:
                        self.log_manager.error(f"Error reading file {status_path}: {exception}")
                        continue

                    if status == 'not completed':
                        self.log_manager.info(f"rsync.status file not found. Skipping stanza: {root}")
                        continue

                    lentochka_status_path = os.path.join(root, 'lentochka-status')

                    if os.path.exists(lentochka_status_path):
                        lentochka_status_count['skipped'] += 1
                        self.log_manager.info(f"Lentochka-status file found in {root}. Skipping stanza.")
                    else:
                        lentochka_status_count['processed'] += 1
                        stanzas.append({
                            'status_path': status_path,
                            'repo_path': root,
                            'status': status
                        })
                        self.log_manager.info(
                            f"Lentochka-status file missing, stanza added to processing queue: {root}")

        self.log_manager.info(
            f"RESULTS: Found {rsync_status_count['completed'] + rsync_status_count['failed'] + rsync_status_count['missing']} rsync.status files, "
            f"successfully copied: {lentochka_status_count['processed']}, skipped: {lentochka_status_count['skipped']}, errors: {rsync_status_count['failed']}")
        return stanzas

    def process_stanza(self, stanza_info):
        try:
            self.lentochka_log.validate_dsmc_log_dir()

            start_time = datetime.datetime.now()

            status_dir = os.path.dirname(stanza_info['status_path'])
            lentochka_status_path = os.path.join(status_dir, 'lentochka-status')

            if os.path.exists(lentochka_status_path):
                with open(lentochka_status_path, 'r') as f:
                    status_content = f.read().strip()
                    if status_content == "":
                        rsync_status_path = stanza_info['status_path']
                        with open(rsync_status_path, 'r') as rsync_file:
                            rsync_status = rsync_file.read().strip().lower()
                            if rsync_status.startswith("complete"):
                                self.lentochka_log.log_lentochka_info(
                                    f"Stanza ({stanza_info['repo_path']}) rsync-status completed, proceeding to copy.")
                                return self.run_dsmc_command(stanza_info, start_time)
                            else:
                                self.lentochka_log.log_lentochka_info(
                                    f"Skipping stanza ({stanza_info['repo_path']}): rsync-status is not 'complete'.")
                                return False
                    else:
                        self.lentochka_log.log_lentochka_info(
                            f"Stanza ({stanza_info['repo_path']}) already processed, skipping.")
                        return True

            else:
                self.lentochka_log.log_lentochka_info(
                    f"Stanza ({stanza_info['repo_path']}) not processed before, starting the copy process.")

                repo_path = stanza_info['repo_path']
                backup_path = os.path.join(repo_path, 'backup')
                archive_path = os.path.join(repo_path, 'archive')

                if not os.path.exists(backup_path) and not os.path.exists(archive_path):
                    self.lentochka_log.log_lentochka_info(
                        f"Skipping stanza ({repo_path}): Neither 'backup' nor 'archive' directories exist.")
                    return False

                result = self.run_dsmc_command(stanza_info, start_time)

                if result == 0:
                    end_time = datetime.datetime.now()
                    status_content = f"Backup written to tape\nStart: {start_time.isoformat()}\nEnd: {end_time.isoformat()}"
                    with open(lentochka_status_path, 'w') as f:
                        f.write(status_content)

                    self.lentochka_log.log_lentochka_info(
                        f"Finished processing stanza {stanza_info['repo_path']} - status: completed, file lentochka-status created."
                    )
                    return True
                else:
                    self.lentochka_log.log_lentochka_error(
                        f"Error processing stanza {stanza_info['repo_path']} - DSMC command failed.")
                    return False

        except Exception as exception:
            self.lentochka_log.log_lentochka_error(f"Uncaught error processing stanza: {exception}")
            return False

    def run_dsmc_command(self, stanza_info, start_time):
        # Убедимся, что директория для логов существует
        rsync_status_dir = self.config.get('Paths', 'rsync_status_dir', fallback='')
        if rsync_status_dir and not os.path.exists(rsync_status_dir):
            os.makedirs(rsync_status_dir)

        log_file_path = self.get_dsmc_log_file_path(stanza_info['repo_path'])

        self.log_manager.info(f"Starting DSMC command at {start_time} for stanza: {stanza_info['repo_path']}")

        dsmc_path = self.config.get('DSMC', 'dsmc_path', fallback='dsmc')
        command = [dsmc_path, 'incr', stanza_info['repo_path']]

        with open(log_file_path, 'w') as log_file:
            process = subprocess.Popen(command, stdout=log_file, stderr=log_file)
            process.communicate()

        self._redirect_to_global_log(log_file_path)

        return process.returncode

    def get_dsmc_log_file_path(self, repo_path):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_filename = f"dsmc-log-{os.path.basename(repo_path)}-{timestamp}.log"
        return os.path.join(self.config.get('Paths', 'rsync_status_dir', fallback='logs'), log_filename)

    def _redirect_to_global_log(self, log_file_path):
        try:
            with open(log_file_path, 'r') as log_file:
                log_content = log_file.read()
                self.log_manager.info(f"DSMCommand Output:\n{log_content}")
        except Exception as e:
            self.log_manager.error(f"Error reading DSMC log file: {e}")

monitoring = None

def main():
    global monitoring
    try:
        # Создаем объект для логирования
        dsmc_log = DsmcPlusLentochkaLogs()

        # Создаем объект для мониторинга
        monitoring = MonitoringHandler(dsmc_log.config, dsmc_log.log_manager)

        # Получаем максимальное количество одновременных экземпляров
        max_instances = dsmc_log.config.getint('Process', 'max_instances', fallback=1)

        # Получаем путь к файлу блокировки
        lock_file = dsmc_log.config.get('Paths', 'lock_file', fallback='/tmp/lentochka_dsmc.lock')

        # Создаем объект для управления блокировкой процесса
        process_locker = ProcessLocker(lock_file, dsmc_log.log_manager, max_instances)

        with process_locker:
            # Проверяем существование скрипта мониторинга
            if monitoring.script and not os.path.exists(monitoring.script):
                dsmc_log.log_manager.error(f"Monitoring script not found at path: {monitoring.script}")
                monitoring.enabled = False

            dsmc_log.log_manager.info("Starting main script execution.")

            # Создаем объект для обработки stanza
            stanza_processor = StanzaProcessor(dsmc_log.config, dsmc_log)

            # Ищем stanza для обработки
            stanzas = stanza_processor.find_stanzas()

            successful_copies = 0
            failed_copies = 0
            skipped_copies = 0

            # Обрабатываем каждую stanza
            for stanza in stanzas:
                rsync_status_path = os.path.join(stanza['repo_path'], 'rsync.status')

                if os.path.exists(rsync_status_path):
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
                else:
                    dsmc_log.log_manager.info(f"No rsync.status file found at path: {rsync_status_path}")
                    skipped_copies += 1
                    if monitoring.enabled:
                        monitoring.send_metric("missing_rsync_status", 1)

            dsmc_log.log_manager.info(f"Results: Found {len(stanzas)} rsync.status files, "
                                      f"successfully copied: {successful_copies}, skipped: {skipped_copies}, errors: {failed_copies}")

            # Очищаем пустые лог-файлы
            dsmc_log.cleanup_empty_logs()

            dsmc_log.log_manager.info("Script has completed its job and now it's going home")

            # Очищаем старые логи
            if monitoring.log_cleanup_enabled:
                monitoring.cleanup_logs(monitoring.log_dir, monitoring.log_retention_days)

    except Exception as e:
        print(f'Error: {e}')
        if 'dsmc_log' in locals() and 'log_manager' in dir(dsmc_log):
            dsmc_log.log_manager.error(f"Uncaught exception: {e}")
        if monitoring and monitoring.enabled:
            try:
                monitoring.send_metric("script_error", 1, "ERROR")
            except:
                pass
        sys.exit(1)


if __name__ == '__main__':
    main()