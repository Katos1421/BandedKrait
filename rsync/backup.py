#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
import shutil

# Настройка логирования
def setup_logging(config):
    log_dir = os.path.expanduser(config['Paths']['log_dir'])
    os.makedirs(log_dir, exist_ok=True)
    
    log_level = getattr(logging, config['Logging']['level'].upper())
    log_file = os.path.join(log_dir, f'rsync_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=log_level,
        format=config['Logging']['message_format'],
        datefmt=config['Logging']['time_format'],
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return log_file

def load_config():
    """Загрузка конфигурации из файла."""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
        
    config.read(config_path)
    
    # Установка значений по умолчанию
    config.setdefault('Paths', {})
    config.setdefault('DSMC', {})
    
    # Пути
    config['Paths']['search_root'] = config.get('Paths', 'search_root', fallback='/')
    config['Paths']['excluded_dirs'] = config.get('Paths', 'excluded_dirs', 
        fallback='/root/.cache,/home/*/.cache,/proc,/sys,/dev,/run,/tmp,/var/cache,/var/tmp')
    
    return config

def find_repo_files(config):
    """Поиск .repo файлов."""
    search_root = config['Paths']['search_root']
    excluded_dirs = config['Paths']['excluded_dirs'].split(',')
    
    repo_dirs = []
    
    try:
        # Построение списка исключений для find
        exclude_args = []
        for excl_dir in excluded_dirs:
            exclude_args.extend(['-path', f'{excl_dir}', '-prune', '-o'])
        
        # Команда find
        find_cmd = ['find', search_root] + exclude_args + ['-type', 'f', '-name', '*.repo', '-print']
        
        result = subprocess.run(find_cmd, capture_output=True, text=True)
        repo_dirs = result.stdout.strip().split('\n') if result.stdout.strip() else []
        
        logging.info(f"Найдено .repo файлов: {len(repo_dirs)}")
        return repo_dirs
    
    except Exception as e:
        logging.error(f"Ошибка поиска .repo файлов: {e}")
        return []

def process_repo_files(repo_files, config):
    """Обработка найденных .repo файлов."""
    backup_dir = os.path.expanduser('~/lentochka_dir')
    
    # Очистка директории перед резервным копированием
    if os.path.exists(backup_dir):
        for item in os.listdir(backup_dir):
            item_path = os.path.join(backup_dir, item)
            try:
                if os.path.isfile(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            except Exception as e:
                logging.error(f"Ошибка при удалении {item_path}: {e}")
    
    os.makedirs(backup_dir, exist_ok=True)
    
    successful_backups = 0
    failed_backups = 0
    
    for repo_file in repo_files:
        try:
            # Создаем путь для резервной копии
            relative_path = os.path.relpath(repo_file, '/')
            backup_path = os.path.join(backup_dir, relative_path)
            
            # Создаем директории если нужно
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            
            # Копируем файл
            shutil.copy2(repo_file, backup_path)
            
            logging.info(f"Создана резервная копия: {repo_file} -> {backup_path}")
            successful_backups += 1
        
        except Exception as e:
            logging.error(f"Ошибка резервного копирования {repo_file}: {e}")
            failed_backups += 1
    
    # Создание статусного файла
    status_file_path = os.path.join(backup_dir, 'lentochka-status')
    with open(status_file_path, 'w') as status_file:
        status_file.write(f"Backup completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        status_file.write(f"Total files: {len(repo_files)}\n")
        status_file.write(f"Successful backups: {successful_backups}\n")
        status_file.write(f"Failed backups: {failed_backups}\n")
    
    return successful_backups, failed_backups

def main():
    try:
        # Загрузка конфигурации
        config = load_config()
        
        # Настройка логирования
        log_file = setup_logging(config)
        
        # Поиск .repo файлов
        repo_files = find_repo_files(config)
        
        # Обработка найденных файлов
        successful_backups, failed_backups = process_repo_files(repo_files, config)
        
        # Логирование результатов
        logging.info(f"Резервное копирование завершено. Успешно: {successful_backups}, Ошибок: {failed_backups}")
        
        # Отправка метрик в мониторинг (если настроен)
        if config.getboolean('Monitoring', 'enabled', fallback=False):
            try:
                monitoring_script = config.get('Monitoring', 'monitoring_script', fallback=None)
                if monitoring_script:
                    subprocess.run([
                        monitoring_script, 
                        "backup_successful_files", 
                        str(successful_backups)
                    ], check=True)
                    subprocess.run([
                        monitoring_script, 
                        "backup_failed_files", 
                        str(failed_backups)
                    ], check=True)
            except Exception as e:
                logging.error(f"Ошибка отправки метрик мониторинга: {e}")
        
    except Exception as e:
        logging.error(f"Критическая ошибка в основном процессе: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
