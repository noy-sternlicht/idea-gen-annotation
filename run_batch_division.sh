#!/bin/bash -x
#SBATCH --time=168:00:00
#SBATCH --gres=gpu:1,vmem:45g
#SBATCH --mem-per-cpu=20g
#SBATCH --mail-user=noy.sternlicht@mail.huji.ac.il
#SBATCH --mail-type=ALL
#SBATCH --job-name=create_recombination_kb

echo $PWD
activate() {
  . $PWD/annotation_env/bin/activate
}

set_env_vars() {
  PYTHONPATH=$PWD/src
  export PYTHONPATH

  HF_DATASETS_CACHE=$PWD/.datasets_cache
  export HF_DATASETS_CACHE

  HF_HOME=$PWD/.hf_home
  export HF_HOME

  CUDA_VISIBLE_DEVICES=7
  export CUDA_VISIBLE_DEVICES
}

activate
set_env_vars

module load cuda
module load nvidia

python3 create_user_batches.py