FROM saiqi/16mb-platform:latest

RUN apt-get install -y python3-lxml ; \
    pip install python-dateutil pytz

RUN mkdir /service 

ADD application /service/application
ADD ./cluster.yml /service

WORKDIR /service
