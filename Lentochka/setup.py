from setuptools import setup, find_packages

setup(
    name='lentochka',
    version='0.1',
    author='Katos1421',
    author_email='your_email@example.com',
    description='Python Package for LINUX and DSMC',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Katos1421/Lentochka',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'psutil',
        'configparser',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'lentochka=Lentochka.SCRIPT.LentochkaDSMC:main',
        ],
    },
    package_data={
        'Lentochka': ['config/*.ini', 'config/*.txt'],
    },
)