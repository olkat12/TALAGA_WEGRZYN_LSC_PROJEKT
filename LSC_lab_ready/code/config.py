"""
config.py - Centralna konfiguracja projektu

Wszystkie parametry eksperymentu w jednym miejscu

"""

import os
from pathlib import Path


SCRATCH = os.environ.get('SCRATCH', '/tmp')
PROJECT_ROOT = Path(SCRATCH) / 'projekt_lsc'

CODE_DIR = PROJECT_ROOT / 'code'
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'results'
LOGS_DIR = RESULTS_DIR / 'logs'

RESULTS_CSV = RESULTS_DIR / 'results.csv'

# Tworzymy katalogi przy imporcie - idempotentnie
for d in [DATA_DIR, RESULTS_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)



# Macierz eksoerymentów
SCALE_FACTORS = [30, 60, 100, 200]


N_REPETITIONS = 3

# Silniki które porównujemy
ENGINES = ['duckdb', 'pandas_naive', 'pandas_optimized']

# Zapytania TPC-H które testujemy
QUERIES = ['Q1', 'Q6', 'Q5', 'Q18']

# Timeout per pojedynczy pomiar. Jak zapytanie trwa dłużej robimy kill
TIMEOUT_SECONDS = 1800  # 30 minut



# EKSPERYMENT 1: DATA SCALING
# Stałe zasoby (RAM=128GB), zmienne SF. Pokazuje gdzie Pandas pada


DATA_SCALING_CONFIG = {
    'sf_values': [30, 60, 100, 200],
    'slurm_mem_gb': 128,     # stałe - dużo RAM żeby Pandas miał szanse
    'slurm_cpus': 8,    
    'engines': ENGINES,
    'queries': QUERIES,
}


# EKSPERYMENT 2: SCALING RAM
# Stały SF=100, zmienny RAM

RAM_SCALING_CONFIG = {
    'sf': 100,            
    'slurm_mem_gb_values': [32, 64, 128, 184],
    'slurm_cpus': 8,           
    'engines': ENGINES,
    'queries': QUERIES,
}



RAM_SAMPLING_INTERVAL_MS = 100


VERIFY_RESULTS = True



# SCHEMA CSV - kolumny w results.csv

CSV_COLUMNS = [
    'run_id',
    'timestamp_start',
    'timestamp_end',
    'hostname',
    'engine',
    'query',
    'sf',
    'data_size_gb',
    'repetition',
    'slurm_mem_gb',
    'slurm_cpus',
    'status',           # ok | oom | timeout | error
    'setup_time_s',     # czas wczytywania danych (osobno)
    'query_time_s',     # czas samego zapytania (osobno)
    'wall_time_s',      # total = setup + query
    'peak_rss_mb',      # peak RAM
    'avg_cpu_percent',  # średnie CPU
    'peak_cpu_percent',
    'disk_read_mb',     # odczyt z dysku
    'disk_write_mb',    # zapis (DuckDB spilling)
    'result_rows',      # ile wierszy zwróciło zapytanie
    'result_hash',    
    'duckdb_version',
    'pandas_version',
    'python_version',
    'error_message',
]


def get_data_path(sf: int) -> Path:

    return DATA_DIR / f'sf{sf}'


def get_table_path(sf: int, table: str) -> Path:

    return get_data_path(sf) / f'{table}.parquet'


TPCH_TABLES = [
    'customer', 'lineitem', 'nation', 'orders',
    'part', 'partsupp', 'region', 'supplier'
]
