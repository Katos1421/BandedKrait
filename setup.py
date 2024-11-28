from setuptools import setup, find_packages

setup(
    name="backup-script",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'psutil',
    ],
    entry_points={
        'console_scripts': [
            'lentochka=backup_script.BrandedKrait(DSMC):main',
        ],
    },
    include_package_data=True,
    package_data={
        '': ['config.ini'],
    },
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',