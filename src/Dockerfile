FROM ubuntu:latest
MAINTAINER dave@ultravioletanalytics.com

RUN apt-get update && apt-get install -y \
	software-properties-common \
	python-software-properties \
&& apt-get update

# Install anything required to execute python scripts
RUN apt-get install -y \
	python \
	python-setuptools \
	python-dev \
	python-pip \
	libmysqlclient-dev \
	cron

# Upgrade pip before installing all the other python packages
RUN pip install --upgrade pip

# Install numpy, pandas, MySQL-python, sqlalchemy, and requests packages
COPY reqs.txt /opt/datagrid/bin/
RUN pip install -r /opt/datagrid/bin/reqs.txt
RUN pip install --upgrade numpy
RUN pip install --upgrade pandas

# Add script files to /opt/datagrid/bin directory
COPY slv_storage.py /opt/datagrid/bin/
COPY slv-extractor.py /opt/datagrid/bin/

# Add crontab file in the bin directory, and prepare the log directory
COPY crontab /opt/datagrid/bin/slv-cron
RUN mkdir /opt/datagrid/log/
RUN touch /opt/datagrid/log/cron.log

# Run crontab
RUN crontab /opt/datagrid/bin/slv-cron
ENTRYPOINT cron && tail -f /opt/datagrid/log/cron.log