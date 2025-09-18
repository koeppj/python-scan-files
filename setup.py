from setuptools import setup, find_packages

setup(
    name='pg_file_indexer',
    version='1.0.0',
    description='PostgreSQL-based file system indexer',
    author='Your Name',
    packages=find_packages(),
    py_modules=['index_files'],
    install_requires=[
        'psycopg2-binary>=2.9.0',
    ],
    entry_points={
        'console_scripts': [
            'pg-file-indexer=index_files:main',
        ],
    },
    python_requires='>=3.7',
)
