# =============================================================================
# DSE Cal-Adapt Climate Data Pipeline
# =============================================================================
# Build:  docker build -t dse/caladapt-pipeline .
# Run:    ./docker/run_docker.sh notebook|shell
# =============================================================================

FROM condaforge/miniforge3:latest

# System deps for geospatial libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /pipeline

# Copy env definition first (layer caching: rebuilds only when yml changes)
COPY envs/py-env.yml envs/py-env.yml

# create conda environment w/ mamba (faster than conda)
RUN mamba env create -f envs/py-env.yml \
    && mamba run -n py-env pip install jupyterlab \
    && mamba run -n py-env python -m ipykernel install --name py-env --display-name "Python (py-env)" \
    && mamba clean -afy

# cpy proj code
COPY lib/ lib/
COPY notebooks/ notebooks/
COPY docker/ docker/
COPY USA_National_Park_Service_Lands_20170930_4993375350946852027/ USA_National_Park_Service_Lands_20170930_4993375350946852027/

# put py-env Python first on PATH so VS Code and jupyter find it
ENV PATH="/opt/conda/envs/py-env/bin:${PATH}"
ENV PYTHONPATH="/pipeline/lib"

# gaurentee that interactive shells activate py-env
RUN echo 'eval "$(conda shell.bash hook)" && conda activate py-env' >> /root/.bashrc

#  make entrypoint executable
RUN chmod +x docker/entrypoint.sh

# expose JupyterLab port
EXPOSE 8888

ENTRYPOINT ["docker/entrypoint.sh"]
CMD ["notebook"]
