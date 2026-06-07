"""
generate_data.py - Generowanie danych TPC-H w formacie Parquet.

DuckDB ma wbudowany generator dbgen z extension 'tpch'. Generujemy raz,
zapisujemy do Parquet, potem oba silniki (DuckDB i Pandas) czytają z tych
samych plików - to gwarantuje uczciwe porównanie.

Dlaczego Parquet a nie .db (DuckDB native)?
- Parquet czyta i DuckDB i Pandas
- Standardowy format w branży, łatwo porównywać z innymi narzędziami
"""

import argparse
import shutil
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent))
import config


# Generuje dane TPC-H dla danego SF i zapisuje do Parquet.
def generate_sf(sf: int, force: bool = False):

    output_dir = config.get_data_path(sf)
    
    if output_dir.exists() and not force:

        existing = list(output_dir.glob('*.parquet'))
        if len(existing) == len(config.TPCH_TABLES):
            print(f"[SF={sf}] Dane już istnieją w {output_dir}")
            return
    
    if force and output_dir.exists():
        print(f"[SF={sf}] --force: usuwam stary katalog {output_dir}")
        shutil.rmtree(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"GENEROWANIE SF={sf}")
    
    # Używamy in-memory bazy DuckDB do generowania, po zakończeniu wszystko znika
    # poza zapisanymi plikami Parquet
    con = duckdb.connect(':memory:')
    
  
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    
    # Generowanie danych w DuckDB
    
    
    gen_start = time.time()
    con.execute(f"CALL dbgen(sf={sf})")
    gen_time = time.time() - gen_start
    print(f"Wygenerowano w {gen_time:.1f}s.")
    
    # Eksport każdej tabeli do osobnego pliku Parquet
    total_export_time = 0
    total_size_mb = 0
    
    for table in config.TPCH_TABLES:
        parquet_path = output_dir / f'{table}.parquet'
        
        n_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        exp_start = time.time()
        con.execute(f"""
            COPY (SELECT * FROM {table}) 
            TO '{parquet_path}' 
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        exp_time = time.time() - exp_start
        
        size_mb = parquet_path.stat().st_size / 1024 / 1024
        total_export_time += exp_time
        total_size_mb += size_mb
        
        print(f"{table:12s}: {n_rows:>12,d} wierszy, "
              f"{size_mb:>8.2f} MB, {exp_time:>6.2f}s")
    
    con.close()
    
    print(f"GOTOWE: SF={sf}")
    print(f"Łączny rozmiar Parquet: {total_size_mb/1024:.2f} GB")
    print(f"Czas generowania: {gen_time:.1f}s")
    print(f"Czas eksportu: {total_export_time:.1f}s")
    print(f"Lokalizacja: {output_dir}")


def main():

    parser = argparse.ArgumentParser(description='Generowanie danych TPC-H do Parquet')
    parser.add_argument('--sf', type=int, nargs='+', required=True,
                        help='Scale factor(y) do wygenerowania np --sf 30')
    parser.add_argument('--force', action='store_true',
                        help='Nadpisz istniejące pliki')
    
    args = parser.parse_args()
    
    total_start = time.time()
    
    for sf in args.sf:
        try:
            generate_sf(sf, force=args.force)
        except Exception as e:
            print(f"Błąd przy SF={sf}: {type(e).__name__}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    total_time = time.time() - total_start
    print(f"\nWszystkie SF wygenerowane w {total_time/60:.1f} minut")


if __name__ == '__main__':
    main()
