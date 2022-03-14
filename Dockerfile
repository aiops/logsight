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
# copy entrypoint.sh
COPY entrypoint.sh .

# Set logsight home dir
ENV LOGSIGHT_HOME="/code/logsight"

ENTRYPOINT [ "./entrypoint.sh" ]
