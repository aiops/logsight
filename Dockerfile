# docker build -t logsightaiowner/logsight-monolith .

# set base image (host OS)
FROM python:3.7

RUN apt-get -y install python3-numpy
RUN apt-get -y install python3-sklearn
RUN apt-get -y install python3-pandas

# set the working directory in the container
WORKDIR /code

COPY requirements.txt .
# install dependencies
RUN pip install -r requirements.txt

# copy code
COPY logsight/ logsight

ENTRYPOINT [ "python3", "-u", "./logsight/run.py", "--cconf", "connections-docker" ]
#ENTRYPOINT [ "bash" ]
