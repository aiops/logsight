# docker build -t logsight/logsight .

# set base image (host OS)
FROM python:3.8

RUN apt-get update && \
    apt-get -y install --no-install-recommends libc-bin && \
    rm -r /var/lib/apt/lists/*

# set the working directory in the container
WORKDIR /code

COPY ../requirements.txt .
# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# copy code
COPY ../logsight logsight
# copy entrypoint.sh
COPY entrypoint.sh .

# Set logsight home dir
ENV LOGSIGHT_HOME="/code/logsight"

ENTRYPOINT [ "./entrypoint.sh" ]
