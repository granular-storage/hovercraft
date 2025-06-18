#!/bin/bash

#SBATCH --job-name=hovercraft_multinode
#SBATCH --partition=gpu
#SBATCH --account=p200633
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-task=0
#SBATCH --mem=16GB
#SBATCH --ntasks=6
#SBATCH --cpus-per-task=8
#SBATCH --time=00:01:00
#SBATCH --output=slurm_output_%j.out
#SBATCH --error=slurm_output_%j.err
#SBATCH --qos=default

echo "Sourcing /etc/profile to set up environment..."
source /etc/profile
echo "Environment sourced."
echo ""

# --- Environment Setup ---
# It's good practice to start from a clean environment
#ml purge
# Load the modules needed for your application (as you did before)
echo "Loading environment modules..."
ml env/release/2024.1
ml OpenMPI/5.0.3-GCC-13.3.0
echo "Modules loaded."
echo ""

# --- Job Information ---
echo "Slurm Job ID: $SLURM_JOB_ID"
echo "Running on nodes: "
scontrol show hostnames $SLURM_JOB_NODELIST
echo "--------------------------"
echo ""

# --- Application Execution ---
# We will run one MPI process on each of the 6 allocated nodes.
# $SLURM_NTASKS will be 6 based on the directives above (nodes * ntasks-per-node).
#
# NOTE: We remove '--oversubscribe' because Slurm has allocated dedicated resources.
# We also remove '--map-by core --bind-to core' because with only one task per node,
# MPI's default process placement is usually optimal. The process has the entire
# node's CPUs at its disposal.
echo "Launching HoverCraft++ with 1,000,000 requests..."

mpirun -np $SLURM_NTASKS ./hovercraft_demo 1000000

#use with 1 node 6 tasks per node
#mpirun --oversubscribe -np 6 --map-by core --bind-to core ./hovercraft_demo 1000000

echo "--------------------------"
echo "Job finished."
