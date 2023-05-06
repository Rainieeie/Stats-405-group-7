#!/usr/bin/env bash

#SBATCH -p short
#SBATCH -t 24:00:00
#SBATCH -n 4
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=2600M
#SBATCH --mail-user=sli826@wisc.edu
#SBATCH --mail-type=END,FAIL


module load R/R-3.5.2

RpackagesDir="R/library" # the R script will install packages here
mkdir --parents "$RpackagesDir" # make directory & parents; no error if exists

Rscript finalproject2.R "$RpackagesDir"
