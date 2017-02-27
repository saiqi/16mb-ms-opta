FROM saiqi/16mb-platform:latest

RUN apt-get install -y libxml2-dev libxslt-dev python-dev ; \
    pip install lxml python-dateutil pytz

RUN mkdir /service 

ADD application /service/application
ADD ./cluster.yml /service

WORKDIR /service
