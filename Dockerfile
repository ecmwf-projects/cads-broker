FROM continuumio/miniconda3

WORKDIR /src/cads-broker

COPY environment.yml /src/cads-broker/

RUN conda install -c conda-forge gcc python=3.11 \
    && conda env update -n base -f environment.yml

COPY . /src/cads-broker

RUN pip install --no-deps -e .
