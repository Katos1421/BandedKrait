from setuptools import setup, find_packages

setup(
    name="backup-script",
    version="0.1",
    packages=find_packages(),  # Находит все папки с кодом
    install_requires=[         # Зависимости, которые должны быть установлены
        'psutil',
    ],
    entry_points={             # Запуск программы через команду в терминале
        'console_scripts': [
            'backup-script=backup_script.main:main',  # Здесь указываем точку входа
        ],
    },
    include_package_data=True,  # Включаем все файлы в пакете (например, config.ini)
    package_data={             # Включаем дополнительные файлы
        '': ['config.ini'],
    },
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    classifiers=[             # Категории, под которые подходит проект
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)