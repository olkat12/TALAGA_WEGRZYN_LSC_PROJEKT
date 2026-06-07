"""
orchestrator.py - Główna pętla eksperymentu

Czyta z config.py macierz konfiguracji i puszcza worker.py jako subprocess
dla każdej kombinacji (engine, query, sf, repetition).

KLUCZOWE: każdy pomiar w osobnym subprocessie. Jak worker padnie z OOM/timeout,
orchestrator żyje dalej i puszcza kolejny pomiar.

Wyniki zapisuje worker.py, orchestrator tylko loguje co się dzieje.
"""

import argparse
import itertools
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import config


def setup_logging(experiment_name: str) -> logging.Logger:

    log_file = config.LOGS_DIR / f'orchestrator_{experiment_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    logger = logging.getLogger('orchestrator')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    
    logger.info(f"Log zapisywany do: {log_file}")
    return logger


def build_matrix(experiment: str, slurm_mem_gb: int, slurm_cpus: int,
                 sf_filter=None, engine_filter=None, query_filter=None) -> list:

    
    if experiment == 'data_scaling':
        cfg = config.DATA_SCALING_CONFIG
        sf_values = cfg['sf_values']
        engines = cfg['engines']
        query_list = cfg['queries']
    elif experiment == 'ram_scaling':
        cfg = config.RAM_SCALING_CONFIG
        sf_values = [cfg['sf']]
        engines = cfg['engines']
        query_list = cfg['queries']
    else:
        raise ValueError(f"Nieznany eksperyment: {experiment}. Dostępne: 'data_scaling', 'ram_scaling'")
    

    if sf_filter is not None:
        sf_values = [sf for sf in sf_values if sf == sf_filter]
    if engine_filter is not None:
        engines = [e for e in engines if e == engine_filter]
    if query_filter is not None:
        query_list = [q for q in query_list if q == query_filter]
    
    matrix = []
    for sf, query, engine, rep in itertools.product(
            sf_values, query_list, engines, range(1, config.N_REPETITIONS + 1)):
        matrix.append({
            'engine': engine,
            'query': query,
            'sf': sf,
            'repetition': rep,
            'slurm_mem_gb': slurm_mem_gb,
            'slurm_cpus': slurm_cpus,
        })
    
    return matrix


def run_single_measurement(cfg: dict, logger: logging.Logger,
                            timeout: int = config.TIMEOUT_SECONDS) -> str:

    run_id = (f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
              f"{cfg['engine']}_{cfg['query']}_sf{cfg['sf']}_r{cfg['repetition']}")
    
    worker_script = Path(__file__).parent / 'worker.py'
    cmd = [
        sys.executable, str(worker_script),
        '--engine', cfg['engine'],
        '--query', cfg['query'],
        '--sf', str(cfg['sf']),
        '--repetition', str(cfg['repetition']),
        '--slurm-mem-gb', str(cfg['slurm_mem_gb']),
        '--slurm-cpus', str(cfg['slurm_cpus']),
        '--run-id', run_id,
    ]
    
    logger.info(f"START {run_id}")
    start = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            timeout=timeout,
            capture_output=True,
            text=True,
        )
        elapsed = time.time() - start
        
        if result.returncode == 0:

            status = 'ok'
            logger.info(f"  OK ({elapsed:.1f}s): {result.stdout.strip()}")
        else:

            status = _detect_worker_failure_and_record(
                cfg, run_id, elapsed, result.returncode, 
                result.stdout, result.stderr, logger
            )
        
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        status = 'timeout'
        logger.error(f"TIMEOUT po {elapsed:.0f}s (limit {timeout}s)")
        _write_failure_to_csv(cfg, run_id, elapsed, 'timeout',
                              f'Subprocess timeout po {elapsed:.0f}s')
        
    except Exception as e:
        elapsed = time.time() - start
        status = 'error'
        logger.error(f"ERROR ({elapsed:.1f}s): {type(e).__name__}: {e}")
        _write_failure_to_csv(cfg, run_id, elapsed, 'error', f'{type(e).__name__}: {e}')
    
    return status


def _detect_worker_failure_and_record(cfg, run_id, elapsed, exit_code, stdout, stderr, logger):


    if _csv_contains_run_id(run_id):
        logger.warning(f"FAILED ({elapsed:.1f}s, exit={exit_code}): "
                      f"worker zapisał wpis: {stdout.strip()}")
        return 'failed'
    

    if exit_code < 0 or exit_code in (137, 143):  # 137 = 128+9, 143 = 128+15
        status = 'oom'
        msg = f'Worker zabity przez kernel (exit={exit_code}). Prawdopodobnie OOM killer.'
        logger.error(f"OOM-KILLED ({elapsed:.1f}s): {msg}")
    else:
        status = 'error'
        msg = f'Worker padł z exit={exit_code}. stderr: {stderr.strip()[:300]}'
        logger.error(f"ERROR ({elapsed:.1f}s, exit={exit_code}): {stderr.strip()[:300]}")
    
    _write_failure_to_csv(cfg, run_id, elapsed, status, msg)
    return status


def _csv_contains_run_id(run_id: str) -> bool:

    if not config.RESULTS_CSV.exists():
        return False
    try:
        with open(config.RESULTS_CSV, 'r') as f:
            return any(run_id in line for line in f)
    except Exception:
        return False


def _write_failure_to_csv(cfg: dict, run_id: str, elapsed: float, 
                          status: str, error_message: str):

    import csv as csv_module
    import socket
    
    row = {col: '' for col in config.CSV_COLUMNS}
    row.update({
        'run_id': run_id,
        'timestamp_start': datetime.now().isoformat(),
        'timestamp_end': datetime.now().isoformat(),
        'hostname': socket.gethostname(),
        'engine': cfg['engine'],
        'query': cfg['query'],
        'sf': cfg['sf'],
        'repetition': cfg['repetition'],
        'slurm_mem_gb': cfg['slurm_mem_gb'],
        'slurm_cpus': cfg['slurm_cpus'],
        'status': status,
        'wall_time_s': round(elapsed, 2),
        'error_message': error_message,
    })
    
    file_exists = config.RESULTS_CSV.exists()
    with open(config.RESULTS_CSV, 'a', newline='') as f:
        writer = csv_module.DictWriter(f, fieldnames=config.CSV_COLUMNS, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description='Orchestrator benchmarków DuckDB vs Pandas')
    parser.add_argument('--experiment', required=True,
                        choices=['data_scaling', 'ram_scaling'])
    parser.add_argument('--slurm-mem-gb', type=int, required=True,
                        help='RAM dostępny w tej sesji SLURM')
    parser.add_argument('--slurm-cpus', type=int, required=True,
                        help='CPU dostępne w tej sesji SLURM')
    parser.add_argument('--sf-filter', type=int, default=None,
                        help='(debug) tylko ten SF')
    parser.add_argument('--engine-filter', type=str, default=None,
                        help='(debug) tylko ten silnik')
    parser.add_argument('--query-filter', type=str, default=None,
                        help='(debug) tylko to zapytanie')
    parser.add_argument('--timeout', type=int, default=config.TIMEOUT_SECONDS,
                        help='Timeout per pomiar w sekundach')
    parser.add_argument('--dry-run', action='store_true',
                        help='Tylko wypisz co byłoby uruchomione, nie wykonuj')
    
    args = parser.parse_args()
    
    logger = setup_logging(args.experiment)
    logger.info(f"EKSPERYMENT: {args.experiment}")
    logger.info(f"SLURM: {args.slurm_mem_gb}GB RAM, {args.slurm_cpus} CPU")
    logger.info(f"Timeout per pomiar: {args.timeout}s")
    
    matrix = build_matrix(
        args.experiment, args.slurm_mem_gb, args.slurm_cpus,
        sf_filter=args.sf_filter,
        engine_filter=args.engine_filter,
        query_filter=args.query_filter,
    )
    
    logger.info(f"Liczba pomiarów do wykonania: {len(matrix)}")
    
    if args.dry_run:
        logger.info("DRY RUN - lista pomiarów:")
        for i, cfg in enumerate(matrix, 1):
            logger.info(f"  {i:3d}. engine={cfg['engine']:20s} query={cfg['query']} "
                        f"sf={cfg['sf']:3d} rep={cfg['repetition']}")
        return
    
    stats = {'ok': 0, 'failed': 0, 'timeout': 0, 'error': 0}
    total_start = time.time()
    
    for i, cfg in enumerate(matrix, 1):
        logger.info(f"--- [{i}/{len(matrix)}] ---")
        status = run_single_measurement(cfg, logger, timeout=args.timeout)
        stats[status] = stats.get(status, 0) + 1
        
        if i % 10 == 0:
            elapsed = time.time() - total_start
            avg_time = elapsed / i
            eta = avg_time * (len(matrix) - i)
            logger.info(f"  [progress] {i}/{len(matrix)} pomiarów, "
                        f"elapsed={elapsed/60:.1f}min, ETA={eta/60:.1f}min")
    

    total_elapsed = time.time() - total_start
    logger.info(f"KONIEC EKSPERYMENTU. Łączny czas: {total_elapsed/60:.1f} minut")
    logger.info(f"Statystyki: {stats}")
    logger.info(f"Wyniki zapisane w: {config.RESULTS_CSV}")


if __name__ == '__main__':
    main()
