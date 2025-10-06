
export VIRTUALENV_NAMESPACE='novacula-env'
export LOGURU_LEVEL="DEBUG"
export VIRTUALENV_PATH=$PWD/$VIRTUALENV_NAMESPACE


#export SLURM_INCLUDE_DIR=/mnt/market_place/slurm_build/build/include
export SLURM_LIB_DIR=/usr/lib

if [ -d "$VIRTUALENV_PATH" ]; then
    echo "$VIRTUALENV_PATH exists."
    source $VIRTUALENV_PATH/bin/activate
else
    virtualenv -p python ${VIRTUALENV_PATH}
    source $VIRTUALENV_PATH/bin/activate
    pip install -e .
fi