FROM java:8
RUN echo "America/New_York" > /etc/timezone \
&& dpkg-reconfigure -f noninteractive tzdata
RUN apt-get update && apt-get install -y --no-install-recommends cron nano

WORKDIR /opt/src/bumblebee
COPY ./ /opt/src/bumblebee


RUN ln -s /opt/src/bumblebee/build/install/bumblebee /opt/bumblebee
RUN ln -s /opt/bumblebee/bin/bumblebee /usr/bin/bumblebee

CMD /opt/bumblebee/bin/bumblebee
