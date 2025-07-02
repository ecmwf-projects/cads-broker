FROM condaforge/miniforge3:23.11.0-0

ARG MODE=stable

WORKDIR /src

COPY ./git-*-repos.py /src/

COPY environment.${MODE} /src/environment
COPY environment-common.yml /src/environment-common.yml
COPY default-enviroment.yaml /src/environment.yml
COPY environment-dask.yml /src/environment-dask.yml

RUN conda install -y -n base -c conda-forge gitpython typer conda-merge

SHELL ["/bin/bash", "-c"]


RUN set -a && source environment \
    && python ./git-clone-repos.py --default-branch \
    cacholote \
    cads-adaptors \
    cads-broker \
    cads-worker

# NOTE: no environment for cads-adaptors as we only use basic features
RUN conda run -n base conda-merge \
    /src/environment.yml \
    /src/environment-common.yml \
    /src/environment-dask.yml \
    /src/cacholote/environment.yml \
    /src/cads-broker/environment.yml \
    /src/cads-worker/environment.yml \
    > /src/combined-environment.yml \
    && conda env update -n base -f /src/combined-environment.yml \
    && conda clean -afy

RUN conda run -n base pip install --no-deps \
    -e /src/cacholote \
    -e /src/cads-broker \
    -e /src/cads-worker

# NOTE: pip install cads-adaptors mandatory dependencies
RUN conda run -n base pip install -e /src/cads-adaptors

CMD broker init-db && broker run
