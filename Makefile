SHELL := /bin/bash
export JAVA_HOME := $(shell /usr/libexec/java_home -v 21)
PYTHON ?= python3.13
PY := .venv/bin/python
export PYSPARK_PYTHON := $(CURDIR)/$(PY)
LOG := data/NASA_access_log_Jul95

all: output.log

.venv/.spark-ready:
	$(PYTHON) -m venv .venv
	.venv/bin/pip install -q pyspark==4.0.1
	touch $@

$(LOG):
	mkdir -p data
	curl -fL --retry 2 -o $(LOG).gz ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
	gunzip $(LOG).gz

output.log: hw5.py $(LOG) | .venv/.spark-ready
	set -o pipefail; $(PY) -u hw5.py | tee $@.tmp
	mv $@.tmp $@

.PHONY: all
