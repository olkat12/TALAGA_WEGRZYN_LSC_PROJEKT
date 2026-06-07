"""
worker.py - Pojedynczy pomiar wydajności (1 silnik x 1 zapytanie x 1 SF).

Uruchamiany jako subprocess przez orchestrator.py.
Mierzy: czas, peak RAM, CPU, dyskowe I/O. Zapisuje wiersz do CSV.

Ten skrypt może paść (OOM, timeout, błąd), ale orchestrator i tak zarejestruje wynik
"""

import argparse
import csv
import gc
import hashlib
import os
import platform
import socket
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path

import psutil


# Worker importuje config i queries z tego samego katalogu
sys.path.insert(0, str(Path(__file__).parent))
import config
import queries


class ResourceMonitor:

    
    def __init__(self, interval_s: float = 0.1):
        self.interval = interval_s
        self.process = psutil.Process(os.getpid())
        
        self.peak_rss_mb = 0.0
        self.cpu_samples = []
        self.io_start = None
        self.io_end = None
        
        self._stop = threading.Event()
        self._thread = None
    
    def _capture_io(self):

        try:
            return self.process.io_counters()
        except (psutil.AccessDenied, AttributeError):
            return None
    
    def _run(self):

        try:
            self.process.cpu_percent(interval=None)
        except psutil.NoSuchProcess:
            return
        
        while not self._stop.is_set():
            try:
                rss_mb = self.process.memory_info().rss / 1024 / 1024
                if rss_mb > self.peak_rss_mb:
                    self.peak_rss_mb = rss_mb
                
                cpu = self.process.cpu_percent(interval=None)
                if cpu > 0:  # pomijamy zerowe (pierwsze)
                    self.cpu_samples.append(cpu)
            except psutil.NoSuchProcess:
                break
            
            self._stop.wait(self.interval)
    
    def start(self):
        self.io_start = self._capture_io()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    
    def stop(self) -> dict:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        self.io_end = self._capture_io()
        
        avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0.0
        peak_cpu = max(self.cpu_samples) if self.cpu_samples else 0.0
        
        disk_read_mb = 0.0
        disk_write_mb = 0.0
        if self.io_start and self.io_end:
            disk_read_mb = (self.io_end.read_bytes - self.io_start.read_bytes) / 1024 / 1024
            disk_write_mb = (self.io_end.write_bytes - self.io_start.write_bytes) / 1024 / 1024
        
        return {
            'peak_rss_mb': round(self.peak_rss_mb, 2),
            'avg_cpu_percent': round(avg_cpu, 2),
            'peak_cpu_percent': round(peak_cpu, 2),
            'disk_read_mb': round(disk_read_mb, 2),
            'disk_write_mb': round(disk_write_mb, 2),
        }


def get_data_size_gb(data_dir: Path) -> float:

    total_bytes = sum(f.stat().st_size for f in data_dir.glob('*.parquet'))
    return round(total_bytes / 1024 / 1024 / 1024, 2)


def get_versions() -> dict:

    import duckdb
    import pandas as pd
    return {
        'duckdb_version': duckdb.__version__,
        'pandas_version': pd.__version__,
        'python_version': platform.python_version(),
    }


def append_row_to_csv(csv_path: Path, row: dict):

    file_exists = csv_path.exists()
    
    with open(csv_path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()

        complete_row = {col: row.get(col, '') for col in config.CSV_COLUMNS}
        writer.writerow(complete_row)


def run_measurement(args) -> dict:

    
    versions = get_versions()
    data_dir = config.get_data_path(args.sf)
    
    if not data_dir.exists():
        return {
            'status': 'error',
            'error_message': f'Brak danych w {data_dir}. Wygeneruj najpierw przez generate_data.py',
            **versions,
        }
    
    data_size_gb = get_data_size_gb(data_dir)
    

    try:
        query_fn = queries.get_query_function(args.engine, args.query)
    except KeyError as e:
        return {
            'status': 'error',
            'error_message': str(e),
            'data_size_gb': data_size_gb,
            **versions,
        }
    

    con = None
    if args.engine == 'duckdb':
        import duckdb
        con = duckdb.connect(':memory:')

        con.execute(f"SET threads TO {args.slurm_cpus}")

        memory_limit_gb = max(1, int(args.slurm_mem_gb * 0.8))
        con.execute(f"SET memory_limit = '{memory_limit_gb}GB'")

        spill_dir = config.RESULTS_DIR / 'duckdb_spill'
        spill_dir.mkdir(exist_ok=True)
        con.execute(f"SET temp_directory = '{spill_dir}'")
    

    gc.collect()
    
    monitor = ResourceMonitor(interval_s=config.RAM_SAMPLING_INTERVAL_MS / 1000)
    
    result_df = None
    setup_time = None
    query_time = None
    status = 'ok'
    error_message = ''
    
    try:
        monitor.start()
        wall_start = time.perf_counter()
        
        result_df, setup_time, query_time = query_fn(con, data_dir)
        
        wall_time = time.perf_counter() - wall_start
        
    except MemoryError as e:
        status = 'oom'
        error_message = f'MemoryError: {e}'
        wall_time = time.perf_counter() - wall_start if 'wall_start' in dir() else 0
    except Exception as e:

        err_str = str(e).lower()
        if 'out of memory' in err_str or 'oom' in err_str or 'memory' in err_str and 'limit' in err_str:
            status = 'oom'
        else:
            status = 'error'
        error_message = f'{type(e).__name__}: {e}\n{traceback.format_exc()[:500]}'
        wall_time = time.perf_counter() - wall_start if 'wall_start' in dir() else 0
    finally:
        resource_metrics = monitor.stop()
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    
  
    result_hash = ''
    result_rows = 0
    if result_df is not None and status == 'ok':
        result_hash = queries.hash_result(result_df)
        result_rows = len(result_df)
    
    return {
        'status': status,
        'setup_time_s': round(setup_time, 4) if setup_time is not None else '',
        'query_time_s': round(query_time, 4) if query_time is not None else '',
        'wall_time_s': round(wall_time, 4),
        'data_size_gb': data_size_gb,
        'result_rows': result_rows,
        'result_hash': result_hash,
        'error_message': error_message,
        **resource_metrics,
        **versions,
    }


def main():
    parser = argparse.ArgumentParser(description='Pojedynczy pomiar benchmarku')
    parser.add_argument('--engine', required=True, choices=config.ENGINES)
    parser.add_argument('--query', required=True, choices=config.QUERIES)
    parser.add_argument('--sf', type=int, required=True)
    parser.add_argument('--repetition', type=int, required=True)
    parser.add_argument('--slurm-mem-gb', type=int, required=True,
                        help='Limit pamięci ustawiony w SLURM (do logowania, nie egzekwowania)')
    parser.add_argument('--slurm-cpus', type=int, required=True,
                        help='Liczba rdzeni ustawiona w SLURM')
    parser.add_argument('--output-csv', type=Path, default=config.RESULTS_CSV)
    parser.add_argument('--run-id', type=str, default=None,
                        help='Unikalny ID pomiaru. Jeśli pusty, generujemy z timestamp')
    
    args = parser.parse_args()
    
    if args.run_id is None:
        args.run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{args.engine}_{args.query}_sf{args.sf}_r{args.repetition}"
    
    timestamp_start = datetime.now().isoformat()
    

    metrics = run_measurement(args)
    
    timestamp_end = datetime.now().isoformat()
    
  
    row = {
        'run_id': args.run_id,
        'timestamp_start': timestamp_start,
        'timestamp_end': timestamp_end,
        'hostname': socket.gethostname(),
        'engine': args.engine,
        'query': args.query,
        'sf': args.sf,
        'repetition': args.repetition,
        'slurm_mem_gb': args.slurm_mem_gb,
        'slurm_cpus': args.slurm_cpus,
        **metrics,
    }

    append_row_to_csv(args.output_csv, row)
    

    print(f"[{args.run_id}] status={row['status']} "
          f"wall_time={row.get('wall_time_s', 'NA')}s "
          f"peak_rss={row.get('peak_rss_mb', 'NA')}MB", flush=True)
    
    sys.exit(0 if row['status'] == 'ok' else 1)


if __name__ == '__main__':
    main()
