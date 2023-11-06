FROM postgres:14

RUN apt-get update && apt-get -y install git build-essential postgresql-server-dev-14

RUN git clone https://github.com/citusdata/pg_cron.git
RUN export PATH=/usr/pgsql-14/bin:$PATH

RUN cd pg_cron && make && PATH=$PATH make install
RUN cd / && \
        rm -rf /pg_cron && \
        apt-get remove -y git build-essential postgresql-server-dev-14 && \
        apt-get autoremove --purge -y && \
        apt-get clean && \
        apt-get purge
RUN echo en_US.UTF-8 UTF-8 > /etc/locale.gen
RUN locale-gen en_US.UTF-8

COPY ./db/run.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/run.sh
