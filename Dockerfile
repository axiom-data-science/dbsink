FROM debian:stretch

LABEL maintainer="Kyle Wilcox <kyle@axiomdatascience.com>" \
      description='Container to run the DB Sink'

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8

RUN apt-get update && apt-get install -y \
        binutils \
        build-essential \
        bzip2 \
        ca-certificates \
        curl \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        libxrender1 \
        wget \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Setup CONDA (https://hub.docker.com/r/continuumio/miniconda3/~/dockerfile/)
ENV MINICONDA_VERSION py38_4.8.2
ENV MINICONDA_SHA256 5bbb193fd201ebe25f4aeb3c58ba83feced6a25982ef4afa86d5506c3656c142
RUN curl -k -o /miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-$MINICONDA_VERSION-Linux-x86_64.sh && \
    echo $MINICONDA_SHA256 /miniconda.sh | sha256sum --check && \
    /bin/bash /miniconda.sh -b -p /opt/conda && \
    rm /miniconda.sh && \
    /opt/conda/bin/conda update -c conda-forge -n base conda && \
    /opt/conda/bin/conda clean -afy && \
    /opt/conda/bin/conda init && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete && \
    /opt/conda/bin/conda install -y -c conda-forge -n base mamba pip && \
    /opt/conda/bin/conda clean -afy

ENV PATH /opt/conda/bin:$PATH

# Install requirements
COPY requirements.txt /tmp/
RUN mamba install -y \
        -c axiom-data-science \
        -c conda-forge \
        --file /tmp/requirements.txt

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

ENV PROJECT_ROOT /code
RUN mkdir -p "$PROJECT_ROOT"
COPY . $PROJECT_ROOT
RUN cd $PROJECT_ROOT && pip install --no-deps .
WORKDIR $PROJECT_ROOT

CMD ["dbsink"]
