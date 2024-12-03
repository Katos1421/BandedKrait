import os
from setuptools import setup, find_packages

# Устанавливаем сам пакет
setup(
    name="lentochka",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'psutil',
        'configparser',
    ],
    entry_points={
        'console_scripts': [
            'lentochka=Branded_Kait.BandedKraitDSMC:main',
        ],
    },
    include_package_data=True,
    package_data={
        '': ['config.ini'],
    },
    long_description=open('readme.md').read() if os.path.exists('readme.md') else 'Описание не найдено.',
    long_description_content_type="text/markdown",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX :: Linux',
    ],
    python_requires='>=3.6',
    author="Воронин Данил Дмитриевич",
    author_email="dvoronin@phoenixit.ru",
    description='Утилита для резервного копирования на ленточные накопители',
    keywords='backup dsmc tape linux system',
    url="https://github.com/Katos1421/BandedKrait",
    project_urls={
        'Bug Tracker': 'https://github.com/Katos1421/BandedKrait/issues',
        'Documentation': 'https://github.com/Katos1421/BandedKrait/wiki',
    },
)