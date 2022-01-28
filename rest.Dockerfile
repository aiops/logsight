# docker build -f rest.Dockerfile -t logsightaiowner/logsight-quality-gate-api:test .

# set base image (host OS)
FROM python:3.7

ENV LDFLAGS="-L/usr/lib/x86_64-linux-gnu"
ENV CFLAGS="-I/usr/include"

# set the working directory in the container
WORKDIR /code

COPY requirements.txt .
# install dependencies
RUN pip install -r requirements.txt

# copy code
COPY logsight/ logsight

ENTRYPOINT [ "python3", "-u", "./logsight/manager_rest.py", "--cconf", "connections-docker" ]
#ENTRYPOINT [ "bash" ]
