"""
queries.py - Implementacje 4 zapytań TPC-H w trzech wariantach.

Warianty:
- DuckDB: czysty SQL, silnik sam optymalizuje
- pandas_naive: wczytuje całe tabele przed filtrowaniem
- pandas_optimized: tylko niezbędne kolumny + predicate pushdown gdzie się da

Każda funkcja zwraca dwukrotnie:
- DataFrame z wynikami (do weryfikacji poprawności)
- czas setup (wczytywania) i query (samego zapytania) osobno
"""

import time
import hashlib
import duckdb
import pandas as pd
from pathlib import Path
from typing import Tuple



QUERY_REGISTRY = {}


def register(engine: str, query: str):

    def decorator(fn):
        QUERY_REGISTRY[(engine, query)] = fn
        return fn
    return decorator


def get_query_function(engine: str, query: str):

    if (engine, query) not in QUERY_REGISTRY:
        raise KeyError(f"Brak implementacji dla {engine}/{query}")
    return QUERY_REGISTRY[(engine, query)]



def _duckdb_setup(con, data_dir: Path, tables: list) -> float:

    start = time.perf_counter()
    for table in tables:
        path = data_dir / f'{table}.parquet'
        con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM '{path}'")
    return time.perf_counter() - start



# Q1 - PRICING SUMMARY REPORT

Q1_SQL = """
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"""


@register('duckdb', 'Q1')
def q1_duckdb(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:
    setup_time = _duckdb_setup(con, data_dir, ['lineitem'])
    
    query_start = time.perf_counter()
    result = con.execute(Q1_SQL).df()
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time


@register('pandas_naive', 'Q1')
def q1_pandas_naive(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:

    setup_start = time.perf_counter()
    lineitem = pd.read_parquet(data_dir / 'lineitem.parquet')
    setup_time = time.perf_counter() - setup_start
    
    query_start = time.perf_counter()
    filtered = lineitem[lineitem['l_shipdate'] <= pd.Timestamp('1998-09-02')].copy()
    filtered['disc_price'] = filtered['l_extendedprice'] * (1 - filtered['l_discount'])
    filtered['charge'] = filtered['disc_price'] * (1 + filtered['l_tax'])
    
    result = filtered.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('l_quantity', 'sum'),
        sum_base_price=('l_extendedprice', 'sum'),
        sum_disc_price=('disc_price', 'sum'),
        sum_charge=('charge', 'sum'),
        avg_qty=('l_quantity', 'mean'),
        avg_price=('l_extendedprice', 'mean'),
        avg_disc=('l_discount', 'mean'),
        count_order=('l_quantity', 'count'),
    ).reset_index().sort_values(['l_returnflag', 'l_linestatus']).reset_index(drop=True)
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time


@register('pandas_optimized', 'Q1')
def q1_pandas_optimized(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:

    setup_start = time.perf_counter()
    needed_cols = ['l_shipdate', 'l_returnflag', 'l_linestatus',
                   'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax']
    lineitem = pd.read_parquet(
        data_dir / 'lineitem.parquet',
        columns=needed_cols,
        filters=[('l_shipdate', '<=', pd.Timestamp('1998-09-02'))]
    )
    setup_time = time.perf_counter() - setup_start
    
    query_start = time.perf_counter()
    lineitem['disc_price'] = lineitem['l_extendedprice'] * (1 - lineitem['l_discount'])
    lineitem['charge'] = lineitem['disc_price'] * (1 + lineitem['l_tax'])
    
    result = lineitem.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('l_quantity', 'sum'),
        sum_base_price=('l_extendedprice', 'sum'),
        sum_disc_price=('disc_price', 'sum'),
        sum_charge=('charge', 'sum'),
        avg_qty=('l_quantity', 'mean'),
        avg_price=('l_extendedprice', 'mean'),
        avg_disc=('l_discount', 'mean'),
        count_order=('l_quantity', 'count'),
    ).reset_index().sort_values(['l_returnflag', 'l_linestatus']).reset_index(drop=True)
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time



# Q6 - FORECASTING REVENUE CHANGE


Q6_SQL = """
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
"""


@register('duckdb', 'Q6')
def q6_duckdb(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:
    
    setup_time = _duckdb_setup(con, data_dir, ['lineitem'])
    
    query_start = time.perf_counter()
    result = con.execute(Q6_SQL).df()
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time


@register('pandas_naive', 'Q6')
def q6_pandas_naive(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:
    
    setup_start = time.perf_counter()
    lineitem = pd.read_parquet(data_dir / 'lineitem.parquet')
    setup_time = time.perf_counter() - setup_start
    
    query_start = time.perf_counter()
    mask = (
        (lineitem['l_shipdate'] >= pd.Timestamp('1994-01-01')) &
        (lineitem['l_shipdate'] < pd.Timestamp('1995-01-01')) &
        (lineitem['l_discount'] >= 0.05) &
        (lineitem['l_discount'] <= 0.07) &
        (lineitem['l_quantity'] < 24)
    )
    revenue = (lineitem.loc[mask, 'l_extendedprice'] * lineitem.loc[mask, 'l_discount']).sum()
    result = pd.DataFrame({'revenue': [revenue]})
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time


@register('pandas_optimized', 'Q6')
def q6_pandas_optimized(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:
    
    setup_start = time.perf_counter()
    lineitem = pd.read_parquet(
        data_dir / 'lineitem.parquet',
        columns=['l_shipdate', 'l_discount', 'l_quantity', 'l_extendedprice'],
        filters=[
            ('l_shipdate', '>=', pd.Timestamp('1994-01-01')),
            ('l_shipdate', '<', pd.Timestamp('1995-01-01')),
        ]
    )
    setup_time = time.perf_counter() - setup_start
    
    query_start = time.perf_counter()
    mask = (
        (lineitem['l_discount'] >= 0.05) &
        (lineitem['l_discount'] <= 0.07) &
        (lineitem['l_quantity'] < 24)
    )
    revenue = (lineitem.loc[mask, 'l_extendedprice'] * lineitem.loc[mask, 'l_discount']).sum()
    result = pd.DataFrame({'revenue': [revenue]})
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time



# Q5 - LOCAL SUPPLIER VOLUME

Q5_SQL = """
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC
"""

Q5_TABLES = ['customer', 'orders', 'lineitem', 'supplier', 'nation', 'region']


@register('duckdb', 'Q5')
def q5_duckdb(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:

    raise NotImplementedError("Implement Q5 for DuckDB")


@register('pandas_naive', 'Q5')
def q5_pandas_naive(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:

    raise NotImplementedError("Implement Q6 for Pandas naive")


@register('pandas_optimized', 'Q5')
def q5_pandas_optimized(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:

    raise NotImplementedError("Implement Q6 for Pandas optimized")



# Q18 - LARGE VOLUME CUSTOMER


Q18_SQL = """
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS total_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey
    FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
)
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"""


@register('duckdb', 'Q18')
def q18_duckdb(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:
    setup_time = _duckdb_setup(con, data_dir, ['customer', 'orders', 'lineitem'])
    
    query_start = time.perf_counter()
    result = con.execute(Q18_SQL).df()
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time


@register('pandas_naive', 'Q18')
def q18_pandas_naive(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:
    setup_start = time.perf_counter()
    customer = pd.read_parquet(data_dir / 'customer.parquet')
    orders = pd.read_parquet(data_dir / 'orders.parquet')
    lineitem = pd.read_parquet(data_dir / 'lineitem.parquet')
    setup_time = time.perf_counter() - setup_start
    
    query_start = time.perf_counter()

    big_orders = lineitem.groupby('l_orderkey', as_index=False)['l_quantity'].sum()
    big_orders = big_orders[big_orders['l_quantity'] > 300]['l_orderkey']
    

    o = orders[orders['o_orderkey'].isin(big_orders)]
    l = lineitem[lineitem['l_orderkey'].isin(big_orders)]
    
    co = customer.merge(o, left_on='c_custkey', right_on='o_custkey')
    col = co.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    
    result = col.groupby(
        ['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'],
        as_index=False
    )['l_quantity'].sum().rename(columns={'l_quantity': 'total_qty'})
    
    result = result.sort_values(
        ['o_totalprice', 'o_orderdate'], ascending=[False, True]
    ).head(100).reset_index(drop=True)
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time


@register('pandas_optimized', 'Q18')
def q18_pandas_optimized(con, data_dir: Path) -> Tuple[pd.DataFrame, float, float]:
    
    setup_start = time.perf_counter()
    customer = pd.read_parquet(data_dir / 'customer.parquet',
                                columns=['c_custkey', 'c_name'])
    orders = pd.read_parquet(data_dir / 'orders.parquet',
                              columns=['o_orderkey', 'o_custkey', 'o_orderdate', 'o_totalprice'])
    lineitem = pd.read_parquet(data_dir / 'lineitem.parquet',
                                columns=['l_orderkey', 'l_quantity'])
    setup_time = time.perf_counter() - setup_start
    
    query_start = time.perf_counter()
    big_orders = lineitem.groupby('l_orderkey', as_index=False)['l_quantity'].sum()
    big_orders_keys = big_orders.loc[big_orders['l_quantity'] > 300, 'l_orderkey']
    
    o = orders[orders['o_orderkey'].isin(big_orders_keys)]
    l = lineitem[lineitem['l_orderkey'].isin(big_orders_keys)]
    
    co = customer.merge(o, left_on='c_custkey', right_on='o_custkey')
    col = co.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    
    result = col.groupby(
        ['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'],
        as_index=False
    )['l_quantity'].sum().rename(columns={'l_quantity': 'total_qty'})
    
    result = result.sort_values(
        ['o_totalprice', 'o_orderdate'], ascending=[False, True]
    ).head(100).reset_index(drop=True)
    query_time = time.perf_counter() - query_start
    
    return result, setup_time, query_time



# WERYFIKACJA WYNIKÓW

def hash_result(df: pd.DataFrame) -> str:

    if df is None or len(df) == 0:
        return 'empty'
    df_copy = df.copy()

    for col in df_copy.select_dtypes(include=['float64', 'float32']).columns:
        df_copy[col] = df_copy[col].round(2)

    df_copy = df_copy.sort_values(by=list(df_copy.columns)).reset_index(drop=True)
    return hashlib.md5(
        pd.util.hash_pandas_object(df_copy, index=False).values.tobytes()
    ).hexdigest()
