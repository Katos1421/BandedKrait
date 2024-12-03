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
from datetime import datetime
from pathlib import Path
import time

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
            return
            
        try:
            cmd = [self.script, metric_name, str(value), status]
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка отправки метрики в мониторинг: {e}")
    
    def cleanup_logs(self):
        """
        Очистка старых логов.
        
        Удаляет файлы логов старше указанного количества дней.
        """
        if not self.log_cleanup_enabled:
            logging.info("Автоматическая очистка логов отключена.")
            return
        
        try:
            log_dir = os.path.expanduser(self.log_dir)
            
            # Создаем директорию, если она не существует
            os.makedirs(log_dir, exist_ok=True)
            
            # Текущая дата
            now = datetime.now()
            
            # Счетчики для статистики
            deleted_files = 0
            total_files = 0
            
            # Перебираем все файлы в директории логов
            for filename in os.listdir(log_dir):
                file_path = os.path.join(log_dir, filename)
                
                # Пропускаем поддиректории
                if os.path.isdir(file_path):
                    continue
                
                total_files += 1
                
                # Получаем время последнего изменения файла
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                # Проверяем, старше ли файл указанного срока
                if (now - file_mtime).days > self.log_retention_days:
                    try:
                        os.remove(file_path)
                        deleted_files += 1
                        logging.info(f"Удален старый лог-файл: {filename}")
                    except Exception as e:
                        logging.error(f"Ошибка удаления файла {filename}: {e}")
            
            # Логируем статистику очистки
            logging.info(f"Очистка логов завершена. Всего файлов: {total_files}, Удалено: {deleted_files}")
            
            # Отправляем метрики в мониторинг
            self.send_metric("log_cleanup_total_files", total_files)
            self.send_metric("log_cleanup_deleted_files", deleted_files)
        
        except Exception as e:
            logging.error(f"Ошибка при очистке логов: {e}")
            self.send_metric("log_cleanup_error", 1, "ERROR")

class ProcessLocker:
    """Класс для контроля и завершения предыдущих процессов."""
    def __init__(self, lock_file_path):
        self.lock_file_path = lock_file_path
        self.pid_file = None
    
    def _find_existing_process(self):
        """Находит существующий процесс по PID-файлу."""
        try:
            if not os.path.exists(self.lock_file_path):
                return None
            
            with open(self.lock_file_path, 'r') as f:
                pid = int(f.read().strip())
            
            # Проверяем, существует ли процесс
            try:
                os.kill(pid, 0)
                return pid
            except OSError:
                # Процесс не существует
                return None
        
        except (IOError, ValueError):
            return None
    
    def terminate_existing_process(self):
        """Завершает существующий процесс."""
        pid = self._find_existing_process()
        
        if pid is not None:
            logger.warning(f"Найден активный процесс с PID {pid}. Завершаю.")
            try:
                # Мягкое завершение
                os.kill(pid, signal.SIGTERM)
                time.sleep(3)  # Даем время на завершение
                
                # Принудительное завершение, если процесс не откликается
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                
                # Удаляем старый pid-файл
                if os.path.exists(self.lock_file_path):
                    os.unlink(self.lock_file_path)
            
            except Exception as e:
                logger.error(f"Ошибка при завершении процесса {pid}: {e}")
    
    def __enter__(self):
        # Завершаем существующий процесс перед созданием нового
        self.terminate_existing_process()
        
        # Создаем новый pid-файл
        self.pid_file = open(self.lock_file_path, 'w')
        self.pid_file.write(str(os.getpid()))
        self.pid_file.flush()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Закрываем и удаляем pid-файл
        try:
            if self.pid_file:
                self.pid_file.close()
            
            if os.path.exists(self.lock_file_path):
                os.unlink(self.lock_file_path)
        except Exception as e:
            logger.error(f"Ошибка при освобождении ресурсов: {e}")

def load_config():
    """
    Загрузка и валидация конфигурации из файла.
    
    Returns:
        configparser.ConfigParser: Объект конфигурации с проверенными параметрами
    """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
        
    config.read(config_path)
    
    # Установка значений по умолчанию
    config.setdefault('Paths', {})
    config.setdefault('Monitoring', {})
    config.setdefault('DSMC', {})
    config.setdefault('Logging', {})
    
    # Пути
    config['Paths']['home_dir'] = config.get('Paths', 'home_dir', fallback=os.path.expanduser('~'))
    config['Paths']['log_dir'] = config.get('Paths', 'log_dir', fallback='logs/')
    config['Paths']['search_root'] = config.get('Paths', 'search_root', fallback='')
    config['Paths']['excluded_dirs'] = config.get('Paths', 'excluded_dirs', 
        fallback='/home/dan/.cache,/home/dan/.local,/proc,/sys,/dev,/run,/tmp')
    
    # Мониторинг
    config['Monitoring']['enabled'] = config.get('Monitoring', 'enabled', fallback='true')
    config['Monitoring']['zabbix_server'] = config.get('Monitoring', 'zabbix_server', fallback='127.0.0.1')
    config['Monitoring']['monitoring_script'] = config.get('Monitoring', 'monitoring_script', 
        fallback='/path/to/monitoring/script.sh')
    config['Monitoring']['interval'] = config.get('Monitoring', 'interval', fallback='300')
    
    # DSMC
    config['DSMC']['dsmc_path'] = config.get('DSMC', 'dsmc_path', fallback='/usr/bin/dsmc')
    config['DSMC']['additional_params'] = config.get('DSMC', 'additional_params', fallback='-quiet')
    config['DSMC']['max_backup_copies'] = config.get('DSMC', 'max_backup_copies', fallback='5')
    
    # Логирование
    config['Logging']['level'] = config.get('Logging', 'level', fallback='INFO')
    config['Logging']['time_format'] = config.get('Logging', 'time_format', 
        fallback='%Y-%m-%d %H:%M:%S')
    config['Logging']['message_format'] = config.get('Logging', 'message_format', 
        fallback='%(asctime)s - %(levelname)s - %(message)s')
    config['Logging']['log_retention_days'] = config.get('Logging', 'log_retention_days', fallback='90')
    config['Logging']['log_cleanup_enabled'] = config.get('Logging', 'log_cleanup_enabled', fallback='true')
    
    # Валидация путей
    log_dir = os.path.expanduser(config['Paths']['log_dir'])
    os.makedirs(log_dir, exist_ok=True)
    
    return config

# Загружаем конфигурацию
config = load_config()

# Задаем основные пути из конфига
HOME_DIR = os.path.expanduser(config.get('Paths', 'home_dir', fallback='~'))
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                      config.get('Paths', 'log_dir', fallback='logs'))
LOCK_FILE = os.path.join(LOG_DIR, 'dsmc_backup.lock')

# Настраиваем директорию для логов
os.makedirs(LOG_DIR, exist_ok=True)

# Настраиваем формат логов из конфига
log_format = config.get('Logging', 'message_format', 
                       fallback='%(asctime)s - %(levelname)s - %(message)s')
time_format = config.get('Logging', 'time_format', 
                        fallback='%Y-%m-%d %H:%M:%S')
formatter = logging.Formatter(log_format, datefmt=time_format)

# Создаем файл лога с меткой времени
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
file_handler = logging.FileHandler(os.path.join(LOG_DIR, f'dsmc_backup_{timestamp}.log'))
file_handler.setFormatter(formatter)

# Настраиваем вывод в консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Инициализируем логгер
logger = logging.getLogger('dsmc_logger')
logger.setLevel(config.get('Logging', 'level', fallback='INFO'))
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Инициализируем обработчик мониторинга
monitoring = MonitoringHandler(config)

def poisk_stanz():
    """Поиск всех станз внутри .repo директорий с проверкой статусов."""
    excluded_paths = [
        '/dev/*', '/proc/*', '/sys/*', '/run/*', 
        '/tmp/*', '/var/lib/*', '/var/cache/*', 
        '/var/tmp/*', '/root/*'
    ]
    
    exclude_args = []
    for path in excluded_paths:
        exclude_args.extend(['-not', '-path', path])
    
    find_command = [
        'find', '/', 
        '-type', 'd', 
        '-name', '.repo'
    ] + exclude_args
    
    try:
        result = subprocess.run(find_command, capture_output=True, text=True, check=True)
        repo_dirs = [d for d in result.stdout.strip().split('\n') if d]
        
        stanzy = []
        for repo_dir in repo_dirs:
            try:
                # Проверяем наличие lentochka-status в .repo
                if any(os.path.exists(os.path.join(repo_dir, d, 'lentochka-status')) 
                       for d in os.listdir(repo_dir)):
                    logger.info(f"Пропускаем .repo {repo_dir}: найден lentochka-status")
                    continue
                
                # Поддиректории для потенциальной обработки
                subdirs = []
                for subdir in os.listdir(repo_dir):
                    subdir_path = os.path.join(repo_dir, subdir)
                    
                    # Проверяем, что это директория
                    if not os.path.isdir(subdir_path):
                        continue
                    
                    # Проверяем наличие rsync.status
                    rsync_status_path = os.path.join(subdir_path, 'rsync.status')
                    if not os.path.exists(rsync_status_path):
                        logger.info(f"Пропускаем {subdir}: отсутствует rsync.status")
                        continue
                    
                    # Проверяем содержимое rsync.status
                    try:
                        with open(rsync_status_path, 'r') as f:
                            status = f.read().strip().lower()
                    except IOError as e:
                        logger.warning(f"Не удалось прочитать {rsync_status_path}: {e}")
                        continue
                    
                    # Список статусов, которые мы пропускаем
                    skip_statuses = ['failed', 'waiting', 'error']
                    if status in skip_statuses:
                        logger.info(f"Пропускаем {subdir}: статус '{status}' в rsync.status")
                        continue
                    
                    # Если все проверки пройдены - добавляем в список
                    subdirs.append(subdir)
                
                # Формируем информацию о станзах
                for subdir in subdirs:
                    stanzy.append({
                        'repo_path': repo_dir,
                        'stanza': subdir,
                        'tip_dir': subdir
                    })
            
            except PermissionError:
                logger.warning(f"Нет доступа к директории: {repo_dir}")
            except Exception as e:
                logger.error(f"Ошибка при обработке .repo директории {repo_dir}: {e}")
        
        logger.info(f"Найдено станз для обработки: {len(stanzy)}")
        
        # Отправляем метрики
        monitoring.send_metric("total_repos_found", len(repo_dirs))
        monitoring.send_metric("stanzas_for_processing", len(stanzy))
        
        return stanzy
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка поиска .repo директорий: {e}")
        monitoring.send_metric("repo_search_error", 1, "ERROR")
        return []
    except Exception as e:
        logger.error(f"Неожиданная ошибка при поиске станз: {e}")
        monitoring.send_metric("stanza_search_error", 1, "ERROR")
        return []

def zapis_na_lentu(repo_path, stanza, tip_dir):
    """Запись станзы из backup или archive директории на ленту."""
    try:
        # Формируем путь источника
        source_dir = os.path.join(repo_path, tip_dir, stanza)
        
        # Получаем путь к DSMC и дополнительные параметры из конфига
        dsmc_path = config.get('DSMC', 'dsmc_path', fallback='dsmc')
        additional_params = config.get('DSMC', 'additional_params', fallback='').split()
        
        # Формируем команду для dsmc
        dsmc_command = [dsmc_path, 'archive'] + additional_params + [source_dir]
        
        # Запускаем dsmc
        logger.info(f"Начинаем запись на ленту: {' '.join(dsmc_command)}")
        result = subprocess.run(dsmc_command, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Запись на ленту успешно завершена: {source_dir}")
            monitoring.send_metric(f"tape_write_success_{tip_dir}", 1)
            return True
        else:
            logger.error(f"Ошибка записи на ленту: {result.stderr}")
            monitoring.send_metric(f"tape_write_error_{tip_dir}", 1, "ERROR")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при записи {tip_dir} на ленту: {e}")
        monitoring.send_metric(f"tape_write_error_{tip_dir}", 1, "ERROR")
        return False

def main():
    """Основная функция скрипта."""
    # Используем блокировщик процесса
    with ProcessLocker(LOCK_FILE):
        try:
            # Ищем станзы для записи на ленту
            stanzy_k_obrabotke = poisk_stanz()

            # Записываем найденные станзы на ленту
            logger.info("Начинаем запись на ленту")
            
            uspeshnye_zapisi = 0
            oshibki_zapisi = 0
            
            for info_stanza in stanzy_k_obrabotke:
                repo_dir = info_stanza['repo_path']
                stanza = info_stanza['stanza']
                tip_dir = info_stanza['tip_dir']
                
                # Записываем на ленту и проверяем результат
                if zapis_na_lentu(repo_dir, stanza, tip_dir):
                    # Создаем lentochka-status ТОЛЬКО после успешной записи
                    status_file = os.path.join(repo_dir, tip_dir, stanza, 'lentochka-status')
                    Path(status_file).touch()
                    
                    uspeshnye_zapisi += 1
                    logger.info(f"{os.path.basename(repo_dir)}\\{stanza} ({tip_dir}) - Запись на ленту завершена")
                else:
                    oshibki_zapisi += 1
                    logger.error(f"{os.path.basename(repo_dir)}\\{stanza} ({tip_dir}) - Ошибка записи на ленту")

            logger.info("Запись на ленту завершена")
            
            try:
                # Основная логика обработки и записи на ленту
                uspeshnye_zapisi, oshibki_zapisi = 0, 0
                
                # Логика обработки и записи...
                
                if oshibki_zapisi > 0:
                    monitoring.send_metric("successful_writes", uspeshnye_zapisi)
                    monitoring.send_metric("failed_writes", oshibki_zapisi)
                    monitoring.send_metric("backup_status", 1, "ERROR")
                else:
                    monitoring.send_metric("successful_writes", uspeshnye_zapisi)
                    monitoring.send_metric("failed_writes", oshibki_zapisi)
                    monitoring.send_metric("backup_status", 0, "OK")
                
                # Вызываем очистку логов
                monitoring.cleanup_logs()

            except Exception as e:
                logger.error(f"Ошибка процесса записи на ленту: {e}")
                monitoring.send_metric("backup_status", 1, "ERROR")
                raise

        except Exception as e:
            logger.error(f"Ошибка процесса записи на ленту: {e}")
            monitoring.send_metric("backup_status", 1, "ERROR")
            raise

if __name__ == "__main__":
    main()
