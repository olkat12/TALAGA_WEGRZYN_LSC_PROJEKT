#!/bin/bash
#SBATCH --job-name=tpch_gen
#SBATCH --time=12:00:00
#SBATCH --mem=128G
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --partition=plgrid
#SBATCH --account=WPISZ_TU_KOD_GRANTU_OD_PROWADZACEGO
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=TWOJ_EMAIL@example.com
#SBATCH --output=projekt_lsc_lab/results/logs/slurm_gen_%j.out
#SBATCH --error=projekt_lsc_lab/results/logs/slurm_gen_%j.err

set -e
echo "JOB ID: $SLURM_JOB_ID"
echo "NODE: $(hostname)"
echo "START: $(date)"


module load miniconda3
eval "$(conda shell.bash hook)"
conda activate projekt_lsc_lab

cd $SCRATCH/projekt_lsc_lab/code

echo "Generowanie SF=30"
python generate_data.py --sf 30

echo "Generowanie SF=60"
python generate_data.py --sf 60

echo "Generowanie SF=100 (~60GB)"
python generate_data.py --sf 100


echo "KONIEC: $(date)"
du -sh $SCRATCH/projekt_lsc_lab/data/*
