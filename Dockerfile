FROM caseyjlaw/bifrostcpu

RUN apt-get update -y && apt-get install -y emacs ipython python-lxml

ADD bf_sdm.py /bifrost

RUN pip install git+https://github.com/realfastvla/sdmpy.git

# need to install rfpipe

WORKDIR /workspace
