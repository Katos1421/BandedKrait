import os
from setuptools import setup, find_packages

# Функция для добавления ~/.local/bin в PATH
def modify_path():
    # Путь, который нужно добавить в PATH
    local_bin_path = os.path.expanduser("~/.local/bin")

    # Получаем текущий PATH
    current_path = os.environ.get('PATH', '')

    # Проверяем, если путь уже добавлен, ничего не делаем
    if local_bin_path not in current_path:
        # Добавляем в системный PATH
        print(f"Добавление {local_bin_path} в PATH.")
        os.environ['PATH'] = current_path + ':' + local_bin_path

        # Пишем в файл конфигурации оболочки (например, .bashrc)
        bashrc_path = os.path.expanduser("~/.bashrc")
        if os.path.exists(bashrc_path):
            with open(bashrc_path, 'a') as f:
                f.write(f"\n# Добавление ~/.local/bin в PATH\n")
                f.write(f"export PATH=$PATH:{local_bin_path}\n")
            print(f"{local_bin_path} добавлен в ваш PATH. Не забудьте перезагрузить оболочку!")
        else:
            print("Не удалось найти файл ~/.bashrc, добавьте вручную в PATH: ", local_bin_path)

# Вызываем функцию при установке
modify_path()

# Устанавливаем сам пакет
setup(
    name="lentochka",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'psutil',
        # 'configparser',  # Не нужно для Python 3
    ],
    entry_points={
        'console_scripts': [
            'lentochka=Branded_Krait.BrandedKraitDSMC:main',
        ],
    },
    include_package_data=True,
    package_data={
        '': ['config.ini'],
    },
    long_description=open('README.md').read() if os.path.exists('README.md') else 'Описание не найдено.',
    long_description_content_type="text/markdown",
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Linux',
    ],
    python_requires='>=3.6',
    # Метаданные проекта
    author="Воронин Данил Дмитриевич",
    author_email="dvoronin@phoenixit.ru",
    url="https://github.com/Katos1421/BandedKrait",
    project_urls={
        'Bug Tracker': 'https://github.com/Katos1421/BandedKrait/issues',
        'Documentation': 'https://github.com/Katos1421/BandedKrait/wiki',
    },
)