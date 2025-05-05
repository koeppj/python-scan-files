from setuptools import setup, find_packages

setup(
    name='pg_file_indexer',
    version='1.0.0',
    description='Asynchronous PostgreSQL-based file system indexer',
    author='Your Name',
    packages=find_packages(),
    py_modules=['index_files'],
    install_requires=[
        'asyncpg>=0.27.0',
    ],
    entry_points={
        'console_scripts': [
            'pg-file-indexer=index_files:main',
        ],
    },
    python_requires='>=3.8',
)
