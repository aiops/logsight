# docker build -t logsight/logsight .

# set base image (host OS)
FROM python:3.7

RUN apt-get update
RUN apt-get -y install libc-bin
#RUN apt-get -y install python3-numpy
RUN apt-get -y install python3-sklearn
RUN apt-get -y install python3-pandas

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
