cwd=/home/researcher/oss/worktrees/ray-review-4
bash -c pwd\;\ git\ branch\ --show-current\;\ git\ rev-parse\ HEAD\ HEAD\^\;\ git\ --no-optional-locks\ status\ --porcelain=v1\ --untracked-files=all\;\ git\ diff\ --stat\;\ git\ diff\ --cached\ --stat 
