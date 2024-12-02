import os
import shutil
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from enum import Enum

# Constants
HOME_DIR = str(Path.home())
LENTOCHKA_DIR = os.path.join(HOME_DIR, 'lentochka_dir')
QUEUE_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'queue.txt')
LOG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logs')

# Region dictionary
REGIONS = {
    'kc': 'Kabardino-Balkar Republic',
    'kr': 'Republic of Karelia',
    'ko': 'Komi Republic',
    'me': 'Mari El Republic',
    'mo': 'Mordovia Republic',
    'sa': 'Sakha Republic (Yakutia)',
    'se': 'North Ossetia-Alania',
    'ta': 'Tatarstan Republic',
    'ty': 'Tuva Republic',
    'ud': 'Udmurt Republic',
    'kk': 'Khakassia Republic',
    'cu': 'Chuvash Republic',
    'alt': 'Altai Krai',
    'kda': 'Krasnodar Krai',
    'kya': 'Krasnoyarsk Krai',
    'pri': 'Primorsky Krai',
    'sta': 'Stavropol Krai',
    'kha': 'Khabarovsk Krai',
    'amu': 'Amur Oblast',
    'ark': 'Arkhangelsk Oblast',
    'ast': 'Astrakhan Oblast',
    'blg': 'Belgorod Oblast',
    'brn': 'Bryansk Oblast',
    'vld': 'Vladimir Oblast',
    'vgg': 'Volgograd Oblast',
    'vlg': 'Vologda Oblast',
    'vrn': 'Voronezh Oblast',
    'iva': 'Ivanovo Oblast',
    'irk': 'Irkutsk Oblast',
    'kgd': 'Kaliningrad Oblast',
    'klg': 'Kaluga Oblast',
    'kam': 'Kamchatka Krai',
    'kem': 'Kemerovo Oblast',
    'kir': 'Kirov Oblast',
    'kst': 'Kostroma Oblast',
    'kgn': 'Kurgan Oblast',
    'ksk': 'Kursk Oblast',
    'lno': 'Leningrad Oblast',
    'lpc': 'Lipetsk Oblast',
    'mag': 'Magadan Oblast',
    'mos': 'Moscow Oblast',
    'mur': 'Murmansk Oblast',
    'niz': 'Nizhny Novgorod Oblast',
    'ngr': 'Novgorod Oblast',
    'nvs': 'Novosibirsk Oblast',
    'oms': 'Omsk Oblast',
    'ore': 'Orenburg Oblast',
    'orl': 'Oryol Oblast',
    'pnz': 'Penza Oblast',
    'per': 'Perm Krai',
    'psk': 'Pskov Oblast',
    'ros': 'Rostov Oblast',
    'rzn': 'Ryazan Oblast',
    'sam': 'Samara Oblast',
    'sar': 'Saratov Oblast',
    'sak': 'Sakhalin Oblast',
    'sve': 'Sverdlovsk Oblast',
    'sml': 'Smolensk Oblast',
    'tmb': 'Tambov Oblast',
    'tvr': 'Tver Oblast',
    'tom': 'Tomsk Oblast',
    'tul': 'Tula Oblast',
    'tyu': 'Tyumen Oblast',
    'uly': 'Ulyanovsk Oblast',
    'che': 'Chelyabinsk Oblast',
    'zab': 'Zabaykalsky Krai',
    'yar': 'Yaroslavl Oblast',
    'msk': 'Moscow',
    'spb': 'Saint Petersburg',
    'yev': 'Jewish Autonomous Oblast',
    'krm': 'Crimea',
    'nen': 'Nenets Autonomous Okrug',
    'khm': 'Khanty-Mansi Autonomous Okrug – Yugra',
    'chu': 'Chukchi Autonomous Okrug',
    'yan': 'Yamalo-Nenets Autonomous Okrug',
    'sev': 'Sevastopol',
    'ce': 'Chechen Republic'
}

class LogStatus:
    """Log statuses"""
    INFO = "INFO"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ERROR = "ERROR"

# Create log directory
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configure logging
log_file = os.path.join(LOG_DIR, f'backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

class CustomFormatter(logging.Formatter):
    def format(self, record):
        # Standard format with exact time
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

logger = logging.getLogger('backup_logger')
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(CustomFormatter())
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(CustomFormatter())
logger.addHandler(console_handler)

def get_region_name(repo_name):
    """Получение полного названия региона по коду."""
    code = repo_name.split('.')[0]
    return REGIONS.get(code, code)

def find_all_stanzas():
    """Находит все станзы во всех репозиториях."""
    stanzas_to_process = []
    unprocessed_status_files = 0
    processed_status_files = 0
    
    logger.info("Starting backup search")
    
    for root, dirs, _ in os.walk(HOME_DIR):
        for dir_name in dirs:
            # Проверяем, что это репозиторий
            if dir_name.endswith('.repo'):
                repo_path = os.path.join(root, dir_name)
                backup_dir = os.path.join(repo_path, 'backup')
                
                if not os.path.exists(backup_dir):
                    continue
                
                for stanza in os.listdir(backup_dir):
                    stanza_path = os.path.join(backup_dir, stanza)
                    status_file = os.path.join(stanza_path, 'lentochka-status')
                    
                    if os.path.isdir(stanza_path):
                        # Получаем timestamp последнего изменения
                        last_modified = datetime.fromtimestamp(os.path.getmtime(stanza_path)).strftime('%Y-%m-%d %H:%M:%S')
                        
                        if os.path.exists(status_file):
                            processed_status_files += 1
                            logger.info(f"Stanza {os.path.basename(repo_path)}\\{stanza} found at {last_modified}, already processed - skipping")
                        else:
                            unprocessed_status_files += 1
                            stanzas_to_process.append({
                                'repo_path': repo_path, 
                                'stanza': stanza,
                                'timestamp': last_modified
                            })
                            logger.info(f"Stanza {os.path.basename(repo_path)}\\{stanza} found at {last_modified}")
    
    logger.info(f"Found unprocessed status files: {unprocessed_status_files}")
    logger.info(f"Found processed status files: {processed_status_files}")
    logger.info(f"Stanzas to process: {len(stanzas_to_process)}")
    
    return stanzas_to_process

def check_rsync_status(repo_path, stanza):
    """Check rsync status."""
    backup_path = os.path.join(repo_path, 'backup', stanza)
    archive_path = os.path.join(repo_path, 'archive', stanza)
    
    if not (os.path.exists(backup_path) and os.path.exists(archive_path)):
        logger.error(f"{os.path.basename(repo_path)}\\{stanza} - Backup/archive not found")
        return False
    
    backup_status = os.path.join(backup_path, 'lentochka-status')
    archive_status = os.path.join(archive_path, 'lentochka-status')
    
    if os.path.exists(backup_status) or os.path.exists(archive_status):
        logger.info(f"{os.path.basename(repo_path)}\\{stanza} already processed")
        return False
    
    rsync_status = os.path.join(backup_path, 'rsync.status')
    if not os.path.exists(rsync_status):
        logger.error(f"{os.path.basename(repo_path)}\\{stanza} - rsync.status not found")
        return False
    
    with open(rsync_status) as f:
        status_line = f.readline().strip()
        parts = status_line.split(';')
        if len(parts) < 2:
            logger.warning(f"{os.path.basename(repo_path)}\\{stanza} - Invalid rsync.status format")
            return False
        
        status, rsync_timestamp = parts[0], parts[1]
        
        if not status.startswith('complete'):
            logger.warning(f"{os.path.basename(repo_path)}\\{stanza} - Status: {status}")
            return False
        
        return rsync_timestamp

def copy_backup(repo_path, stanza, rsync_timestamp):
    """Copy backup."""
    try:
        backup_path = os.path.join(repo_path, 'backup', stanza)
        backup_dest = os.path.join(LENTOCHKA_DIR, f"{os.path.basename(repo_path)}_{stanza}")
        
        os.makedirs(backup_dest, exist_ok=True)
        
        subprocess.run([
            'rsync', 
            '-avz', 
            '--delete', 
            '-q',  # Quiet mode
            f'{backup_path}/', 
            f'{backup_dest}/'
        ], check=True)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"{os.path.basename(repo_path)}\\{stanza} - Copy error: {e}")
        return False
    
    return True

def process_repos(stanzas):
    """Обработка найденных репозиториев."""
    logger.info("Starting repository copying")
    
    for stanza_info in stanzas:
        repo_dir = stanza_info['repo_path']
        stanza = stanza_info['stanza']
        
        backup_src = os.path.join(repo_dir, 'backup', stanza)
        backup_dest = os.path.join(LENTOCHKA_DIR, f"{os.path.basename(repo_dir)}_{stanza}")
        
        rsync_command = f"rsync -avz --delete {backup_src}/ {backup_dest}"
        logger.info(f"{os.path.basename(repo_dir)}\\{stanza} - Copy started: {rsync_command}")
        
        copy_backup(repo_dir, stanza, check_rsync_status(repo_dir, stanza))
    
    logger.info("Repository copying completed")

def clear_lentochka_dir():
    """Clear destination directory."""
    if os.path.exists(LENTOCHKA_DIR):
        for item in os.listdir(LENTOCHKA_DIR):
            item_path = os.path.join(LENTOCHKA_DIR, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.unlink(item_path)
        logger.info(f"Directory {LENTOCHKA_DIR} cleaned")
    else:
        os.makedirs(LENTOCHKA_DIR)
        logger.info(f"Directory {LENTOCHKA_DIR} created")

def remove_lentochka_status():
    """Remove lentochka-status from all found repositories."""
    for root, dirs, _ in os.walk(HOME_DIR):
        for dir_name in dirs:
            if dir_name.endswith('.repo'):
                repo_path = os.path.join(root, dir_name)
                backup_dir = os.path.join(repo_path, 'backup')
                archive_dir = os.path.join(repo_path, 'archive')
                
                for stanza_dir in [backup_dir, archive_dir]:
                    if os.path.exists(stanza_dir):
                        for stanza in os.listdir(stanza_dir):
                            stanza_path = os.path.join(stanza_dir, stanza)
                            if os.path.isdir(stanza_path):
                                status_file = os.path.join(stanza_path, 'lentochka-status')
                                if os.path.exists(status_file):
                                    os.remove(status_file)
                                    logger.info(f"Removed {status_file}")

def main():
    """Основная функция выполнения скрипта."""
    try:
        # Очищаем директорию назначения
        if os.path.exists(LENTOCHKA_DIR):
            shutil.rmtree(LENTOCHKA_DIR)
        os.makedirs(LENTOCHKA_DIR, exist_ok=True)
        logger.info("Destination directory /home/dan/lentochka_dir has been cleaned")

        # Находим бэкапы
        repos_to_process = find_all_stanzas()

        # Начинаем копирование репозиториев
        logger.info("Starting repository copying")
        
        # Создаем список для хранения информации о перемещенных станзах
        moved_stanzas = []
        
        for stanza_info in repos_to_process:
            repo_dir = stanza_info['repo_path']
            stanza = stanza_info['stanza']
            timestamp = stanza_info['timestamp']
            
            # Начинаем копирование
            backup_src = os.path.join(repo_dir, 'backup', stanza)
            backup_dest = os.path.join(LENTOCHKA_DIR, f"{os.path.basename(repo_dir)}_{stanza}")
            
            rsync_command = f"rsync -avz --delete {backup_src}/ {backup_dest}"
            logger.info(f"{os.path.basename(repo_dir)}\\{stanza} - Copy started: {rsync_command}")
            
            # Выполняем копирование бэкапа
            if copy_backup(repo_dir, stanza, check_rsync_status(repo_dir, stanza)):
                logger.info(f"{os.path.basename(repo_dir)}\\{stanza} - Copy completed")
                
                # Сохраняем информацию о перемещенной станзе
                moved_stanzas.append({
                    'repo': os.path.basename(repo_dir),
                    'stanza': stanza,
                    'timestamp': timestamp
                })
            else:
                logger.error(f"{os.path.basename(repo_dir)}\\{stanza} - Copy failed")

        logger.info("Repository copying completed")

    except Exception as e:
        logger.error(f"Backup process failed: {e}")
        raise

if __name__ == "__main__":
    main()
