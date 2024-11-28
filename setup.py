import os
from setuptools import setup, find_packages

setup(
    name="backup-script",
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
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
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