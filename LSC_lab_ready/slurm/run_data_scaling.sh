#!/bin/bash
#SBATCH --job-name=bench_data
#SBATCH --time=14:00:00
#SBATCH --mem=128G
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --partition=plgrid
#SBATCH --account=WPISZ_TU_KOD_GRANTU_OD_PROWADZACEGO
#SBATCH --output=projekt_lsc_lab/results/logs/slurm_bench_data_%j.out
#SBATCH --error=projekt_lsc_lab/results/logs/slurm_bench_data_%j.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=TWOJ_EMAIL@example.com

set -e
echo "EKSPERYMENT: DATA SCALING"
echo "JOB ID: $SLURM_JOB_ID"
echo "START: $(date)"


module load miniconda3
eval "$(conda shell.bash hook)"
conda activate projekt_lsc_lab

cd $SCRATCH/projekt_lsc_lab/code

python orchestrator.py \
    --experiment data_scaling \
    --slurm-mem-gb 128 \
    --slurm-cpus 8 \
    --timeout 1800


echo "KONIEC: $(date)"