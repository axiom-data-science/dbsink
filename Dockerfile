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
ENV MINICONDA_VERSION latest
ARG PYTHON_VERSION=3.7
RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    curl -k -o /miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-$MINICONDA_VERSION-Linux-x86_64.sh && \
    /bin/bash /miniconda.sh -b -p /opt/conda && \
    rm /miniconda.sh && \
    /opt/conda/bin/conda config \
        --set always_yes yes \
        --set changeps1 no \
        --set show_channel_urls True \
        && \
    /opt/conda/bin/conda config \
        --add channels axiom-data-science \
        --add channels conda-forge \
        && \
    /opt/conda/bin/conda install python=$PYTHON_VERSION && \
    /opt/conda/bin/conda clean -a -y

ENV PATH /opt/conda/bin:$PATH

# Install requirements
COPY requirements.txt /tmp/
RUN conda install -y \
        --file /tmp/requirements.txt \
        && \
    conda clean -a -y

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
