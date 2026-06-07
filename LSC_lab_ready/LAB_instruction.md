# Lab Exercise: Optimizing analytical data processing – DuckDB vs Pandas


**Max Points:** 50 pts  

---

## Objective
The goal of this laboratory exercise is to get experience with the architecture of modern analytical database engines processing data within a single thick compute node (Scale-up architecture). You will investigate the **Memory cliff** phenomenon in the Pandas library and the stable boundary handling in the DuckDB engine using the official industry standard **TPC-H** benchmark.

This lab simulates real-world conditions of a data engineer working in HPC environment on the Cyfronet Ares cluster using the **SLURM** workload manager.

---

## System architecture and project structure

Upload the project files to your `$SCRATCH` directory on the Ares cluster, maintaining the following structure exactly:

```text
projekt_lsc_lab/
├── code/
│   ├── config.py              # Central configuration of experiment parameters
│   ├── queries.py             # TPC-H queries (ASSIGNMENT: Q5 REQUIRES IMPLEMENTATION)
│   ├── generate_data.py       # TPC-H data generation algorithm 
│   ├── worker.py              # Script executing a single measurement
│   └── orchestrator.py        # Main loop automating the execution matrix
├── slurm/
│   ├── generate_data.sh       # sbatch script: data generation
│   ├── run_data_scaling.sh    # sbatch script: Experiment 1 (Data scaling)
│   └── run_ram_scaling.sh     # sbatch script: Experiment 2 (Memory scaling)
└── LAB_instructions.html      # Detailed lab manual and assignment tasks for students
```

Note: The `data/` folder and `results/` folder will be automatically created during the execution pipeline


## Task 1: Environment & data preparation (5 pts)

1. Log in to the login node of the Ares cluster and create an isolated Anaconda environment:

```text
module load miniconda3
conda create -n projekt_lsc_lab python=3.10 pandas pyarrow duckdb psutil -y
```

2. Adapt the universal SLURM scripts located in `code/slurm/` to your own compute account. In the files `generate_data.sh`, `run_data_scaling.sh`, and `run_ram_scaling.sh`, fill in your assigned allocation grant and email address:

```text
#SBATCH --account=ENTER_YOUR_NAME_HERE
#SBATCH --mail-user=YOUR_EMAIL@example.com
```

3. Submit the batch job to generate the synthetic TPC-H datasets:

```text
cd $SCRATCH/projekt_lsc_lab/code
sbatch slurm/generate_data.sh
```

Data will be generated for scale factors SF=30, SF=60, SF=100 and SF=200, and exported into the highly optimized column-oriented `Parquet` format compressed.

## Task 2: Implementing Query Q5 - multi-table join (25 pts)

Open the file `code/queries.py`. Your primary engineering challenge is to complete the missing implementations for the heavily demanding **TPC-H Q5** query. This query joins 6 relational tables(customer, orders, lineitem, supplier, nation, region), filters transactions for a specific region ('ASIA') within a strict one-year time frame, groups the results, and sorts them by revenue.

Use the provided, fully functional implementations of Q1, Q6, and Q18 as architectural templates. You must write three separate variants of the same business logic:
1. **q5_duckdb (5 pts)**: Execute the query using native SQL passed directly to the DuckDB engine.
2. **q5_pandas_naive (10 pts)**: The traditional in-memory approach. Load all 6 required Parquet files entirely into RAM using `pd.read_parquet()` and perform the downstream operations via standard `.merge()`, `.query()`, and `.groupby()` methods.
3. **q5_pandas_optimized (10 pts)**: The memory-optimized Pandas approach. Minimize the structural footprint of the DataFrames by taking advantage of the Parquet format's physical features:
   * **Column Pruning**: Use the `columns` parameter to load only the specific columns involved in the math from the disk.
   * **Predicate Pushdown**: Apply the filter constraints on order dates (o_orderdate) directly at the I/O storage reader layer using the `filters` parameter, reducing the volume of data instantiated into RAM.

**Validation Check:** The execution harness automatically verifies the integrity of your calculations using MD5 checksums. The results from DuckDB and both Pandas variants must be completely identical. Before submitting a heavy batch job, you can run a localized unit test inside an interactive node (`srun`):
```text
python worker.py --engine duckdb --query Q5 --sf 30 --repetition 1 --slurm-mem-gb 16 --slurm-cpus 2
```

## Task 3: Running the benchmark and analyzing results (20 pts)

Once your Q5 implementation passes validation, submit the full experimental workloads to the SLURM queue:

* **Experiment 1 (Data scaling):** Fixed hardware footprint (128 GB RAM), increasing data scales (`SF=30, 60, 100, 200`):
```text
sbatch slurm/run_data_scaling.sh
```

* **Experiment 2 (RAM scaling):** Fixed data scale (SF=100), restricted memory limits enforced via SLURM cgroups partitions (RAM=32, 64, 128, 180 GB):

```text
sbatch --mem=32G  --export=RAM_GB=32  slurm/run_ram_scaling.sh
sbatch --mem=64G  --export=RAM_GB=64  slurm/run_ram_scaling.sh
sbatch --mem=128G --export=RAM_GB=128 slurm/run_ram_scaling.sh
sbatch --mem=180G --export=RAM_GB=180 slurm/run_ram_scaling.sh
```

Upon successful completion of all jobs, download file results/results.csv to your local environment and compile a comprehensive formal lab report (PDF). The report must include:

- Data scaling chart: Execution time comparisons for SF=30, 60, 100, 200 at RAM=128GB. Highlight and document the exact memory limit threshold triggering OOM failures in Pandas.

- RAM scaling chart: An explicit look at peak memory utilization (peak_rss_mb) vs storage writes (disk_write_mb) for the static SF=100 data target across decreasing hardware allocations.

- Architectural discussion: Using your results, explain the mechanisms of DuckDB's out-of-core execution layer that enabled it to cleanly complete workloads where the raw dataset outgrew available physical memory (e.g. 32 GB RAM vs SF=100 data), contrasting it with why Pandas failed.

## 4. Grading criteria

Task 1 (5 pts): Correct cluster configuration, environment generation, and clean, error-free generation of the Parquet file structures.

Task 2 (25 pts): Algorithmic implementation within queries.py. Full verification against the integrity checking framework (MD5 compliance).

Task 3 (20 pts): Analytical quality of the final report. Correct filtering and isolation of overlapping datasets using the timestamp metrics, clarity of plots, and technical understanding of the core concepts (Memory cliff vs out-of-core execution).