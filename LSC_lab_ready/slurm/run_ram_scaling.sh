#!/bin/bash
#SBATCH --job-name=bench_ram
#SBATCH --time=06:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --partition=plgrid
#SBATCH --account=WPISZ_TU_KOD_GRANTU_OD_PROWADZACEGO
#SBATCH --output=projekt_lsc_lab/results/logs/slurm_bench_ram_%j.out
#SBATCH --error=projekt_lsc_lab/results/logs/slurm_bench_ram_%j.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=TWOJ_EMAIL@example.com

set -e

if [ -z "$RAM_GB" ]; then
    echo "BŁĄD: zmienna RAM_GB nie ustawiona"
    exit 1
fi

echo "EKSPERYMENT: RAM SCALING (RAM=${RAM_GB}GB)"
echo "JOB ID: $SLURM_JOB_ID"
echo "START: $(date)"

module load miniconda3
eval "$(conda shell.bash hook)"
conda activate projekt_lsc_lab

cd $SCRATCH/projekt_lsc_lab/code

python orchestrator.py \
    --experiment ram_scaling \
    --slurm-mem-gb $RAM_GB \
    --slurm-cpus 8 \
    --timeout 1800

echo "KONIEC: $(date)"