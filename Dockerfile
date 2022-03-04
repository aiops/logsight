# docker build -t logsight/logsight .

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
COPY logsight-eula.txt /
ENTRYPOINT [ "python3", "-u", "./logsight/run.py" ]
#ENTRYPOINT [ "bash" ]
