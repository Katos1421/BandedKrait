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

def find_config():
    """
    Ищет config.ini в директории скрипта.
    Возвращает путь к конфигу или вызывает исключение.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.ini')
    
    if not os.path.exists(config_path):
        error_msg = f"""
        ОШИБКА: Файл конфигурации не найден!
        
        Скрипт ищет config.ini в директории:
        {script_dir}
        
        Убедитесь, что файл config.ini находится в той же директории,
        что и скрипт LentochkaDSMC.py
        """
        raise FileNotFoundError(error_msg)
    
    return config_path

# Определяем путь к конфигу
try:
    CONFIG_FILE = find_config()
except FileNotFoundError as e:
    print(str(e))
    sys.exit(1)

def load_config():
    """
    Загружает и валидирует конфигурацию.
    
    Returns:
        dict: Словарь с параметрами конфигурации
    """
    # Загружаем конфиг
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    
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
                                                     fallback='%(asctime)s - %(levelname)s - %(message)s')
    config['Logging']['time_format'] = config.get('Logging', 'time_format', 
                                                  fallback='%Y-%m-%d %H:%M:%S')
    config['Logging']['log_retention_days'] = config.get('Logging', 'log_retention_days', fallback='90')
    config['Logging']['log_cleanup_enabled'] = config.get('Logging', 'log_cleanup_enabled', fallback='true')
    
    # Мониторинг
    config['Monitoring']['enabled'] = config.get('Monitoring', 'enabled', fallback='true')
    config['Monitoring']['zabbix_server'] = config.get('Monitoring', 'zabbix_server', fallback='127.0.0.1')
    config['Monitoring']['monitoring_script'] = config.get('Monitoring', 'monitoring_script', 
        fallback='/path/to/monitoring/script.sh')
    config['Monitoring']['interval'] = config.get('Monitoring', 'interval', fallback='300')
    
    return config

def validate_and_prepare_log_dir(log_dir):
    """
    Проверяет и подготавливает директорию для логов.
    
    Args:
        log_dir (str): Путь к директории логов
    
    Returns:
        str: Абсолютный путь к директории логов
    
    Raises:
        ValueError: Если log_dir не указан
        PermissionError: Если нет прав на создание директории
    """
    # Проверяем, указан ли log_dir
    if not log_dir or log_dir.strip() == '':
        error_msg = """
        ОШИБКА: НЕ ЗАДАНА ДИРЕКТОРИЯ ДЛЯ ЛОГОВ В КОНФИГУРАЦИОННОМ ФАЙЛЕ!
        
        В файле config.ini в секции [Paths] необходимо указать параметр log_dir.
        Пример:
        [Paths]
        log_dir = /var/log/Lentochka
        
        Без указания этого пути скрипт не может работать!
        """
        logger.error(error_msg)
        monitoring.send_metric("log_dir_error", 1, "ERROR")
        raise ValueError(error_msg)
    
    # Расширяем путь (обрабатываем ~, переменные окружения)
    log_dir = os.path.expanduser(log_dir.strip())
    
    # Проверяем существование директории
    if not os.path.exists(log_dir):
        try:
            # Пытаемся создать директорию
            os.makedirs(log_dir, exist_ok=True)
            logger.info(f"Создана директория для логов: {log_dir}")
        except PermissionError:
            error_msg = f"ОШИБКА: Нет прав для создания директории логов: {log_dir}"
            logger.error(error_msg)
            monitoring.send_metric("log_dir_permission_error", 1, "ERROR")
            raise PermissionError(error_msg)
    
    # Проверяем, что это директория
    if not os.path.isdir(log_dir):
        error_msg = f"ОШИБКА: Указанный путь не является директорией: {log_dir}"
        logger.error(error_msg)
        monitoring.send_metric("log_dir_not_dir_error", 1, "ERROR")
        raise NotADirectoryError(error_msg)
    
    # Проверяем права на запись
    if not os.access(log_dir, os.W_OK):
        error_msg = f"ОШИБКА: Нет прав на запись в директорию логов: {log_dir}"
        logger.error(error_msg)
        monitoring.send_metric("log_dir_write_error", 1, "ERROR")
        raise PermissionError(error_msg)
    
    return log_dir

def generate_dsmc_log_path(log_dir):
    """
    Генерирует путь к логу DSMC с таймстампом.
    
    Args:
        log_dir (str): Базовая директория для логов
    
    Returns:
        str: Полный путь к логу DSMC
    """
    # Генерируем имя лог-файла с таймстампом
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dsmc_log_filename = f"dsmc_log_{timestamp}.log"
    dsmc_log_path = os.path.join(log_dir, dsmc_log_filename)
    
    return dsmc_log_path

def find_stanzas(search_root):
    """
    Рекурсивный поиск станз с максимальным охватом.
    
    Алгоритм:
    1. Рекурсивно обходит указанную директорию
    2. Ищет файлы rsync.status как маркер станзы
    3. Работает с любой вложенностью директорий
    4. Не привязывается к именам директорий
    
    Args:
        search_root (str): Корневая директория для поиска
    
    Returns:
        list: Список путей к станзам
    """
    stanzas = []
    
    # Расширяем путь, если используются переменные окружения
    search_root = os.path.expanduser(search_root)
    
    # Проверяем существование директории
    if not os.path.exists(search_root):
        logger.error(f"Указанная директория поиска не существует: {search_root}")
        return stanzas
    
    # Рекурсивный обход всех поддиректорий
    for root, dirs, files in os.walk(search_root):
        # Ищем rsync.status как маркер станзы
        if 'rsync.status' in files:
            # Проверяем содержимое rsync.status
            status_path = os.path.join(root, 'rsync.status')
            try:
                with open(status_path, 'r') as f:
                    status = f.read().strip().lower()
                    
                    # Пропускаем станзы с проблемными статусами
                    if status in ['failed', 'waiting', 'error']:
                        logger.info(f"Пропускаем станзу {root}: статус '{status}' в rsync.status")
                        continue
                    
                    # Добавляем станзу
                    stanzas.append(os.path.dirname(status_path))
                    logger.info(f"Найдена станза: {os.path.dirname(status_path)}")
            
            except IOError as e:
                logger.warning(f"Не удалось прочитать {status_path}: {e}")
    
    return stanzas

def poisk_stanz():
    """Поиск всех станз с проверкой статусов."""
    try:
        # Получаем и проверяем корневую директорию поиска из конфига
        search_root = config.get('Paths', 'search_root', fallback=None)
        if not search_root or search_root.strip() == '':
            error_msg = """
            ОШИБКА: НЕ ЗАДАН ПУТЬ ДЛЯ ПОИСКА В КОНФИГУРАЦИОННОМ ФАЙЛЕ!
            
            В файле config.ini в секции [Paths] необходимо указать параметр search_root.
            Пример:
            [Paths]
            search_root = /путь/к/директории/с/бэкапами
            
            Без указания этого пути скрипт не может работать!
            """
            logger.error(error_msg)
            monitoring.send_metric("config_error", 1, "ERROR")
            raise ValueError(error_msg)
        
        # Находим станзы
        stanzy = [
            {
                'repo_path': os.path.dirname(os.path.dirname(stanza)),
                'stanza': os.path.basename(stanza),
                'tip_dir': os.path.basename(stanza)
            }
            for stanza in find_stanzas(search_root)
        ]
        
        logger.info(f"Найдено станз для обработки: {len(stanzy)}")
        monitoring.send_metric("stanzas_for_processing", len(stanzy))
        
        return stanzy
    
    except (ValueError, FileNotFoundError, NotADirectoryError) as e:
        # Эти ошибки уже обработаны выше
        return []
    except Exception as e:
        logger.error(f"Неожиданная ошибка при поиске станз: {e}")
        monitoring.send_metric("stanza_search_error", 1, "ERROR")
        return []

def zapis_na_lentu(repo_path, stanza, tip_dir):
    """Запись станзы из backup или archive директории на ленту."""
    try:
        # Получаем и валидируем log_dir
        log_dir = config.get('Paths', 'log_dir')
        log_dir = validate_and_prepare_log_dir(log_dir)
        
        # Генерируем путь к логу DSMC
        dsmc_log = generate_dsmc_log_path(log_dir)
        
        # Получаем пути к директориям backup и archive
        backup_dir = os.path.join(repo_path, 'backup', stanza)
        archive_dir = os.path.join(repo_path, 'archive', stanza)
        
        # Получаем путь к DSMC
        dsmc_path = config.get('DSMC', 'dsmc_path', fallback='dsmc')
        
        # Формируем команду для выполнения в shell
        dsmc_cmd = f"{dsmc_path} incr {backup_dir} {archive_dir} -su=yes >> {dsmc_log} 2>&1"
        
        logger.info(f"Выполняем команду: {dsmc_cmd}")
        logger.info(f"Лог DSMC будет сохранен в: {dsmc_log}")
        
        # Выполняем команду через shell с перенаправлением потоков
        result = subprocess.run(dsmc_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Запись на ленту успешно завершена: {backup_dir} и {archive_dir}")
            logger.info(f"Лог DSMC сохранен в: {dsmc_log}")
            monitoring.send_metric(f"tape_write_success_{tip_dir}", 1)
            return True
        else:
            logger.error(f"Ошибка записи на ленту. Код возврата: {result.returncode}")
            logger.error(f"Stdout: {result.stdout}")
            logger.error(f"Stderr: {result.stderr}")
            logger.error(f"Полный лог DSMC находится в: {dsmc_log}")
            monitoring.send_metric(f"tape_write_error_{tip_dir}", 1, "ERROR")
            return False
            
    except (ValueError, PermissionError, NotADirectoryError) as e:
        logger.error(f"Ошибка при работе с директорией логов: {e}")
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
    # Загружаем конфигурацию
    config = load_config()

    # Задаем основные пути из конфига
    LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                          config.get('Paths', 'log_dir'))
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

    main()
