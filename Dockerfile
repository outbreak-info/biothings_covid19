FROM alpine:3.11

RUN apk upgrade --update && \
    apk add --no-cache --virtual .build-deps $BUILD_DEPS && \
    apk add --no-cache --virtual .persistent-deps $PERSISTENT_DEPS && \
    apk add --update --no-cache R R-dev && \
    apk add --update --no-cache gcc g++ pkgconfig && \
    apk add --update --no-cache python3 python3-dev freetype-dev libpng musl-dev lapack-dev &&\
    apk add --update --no-cache gdal gdal-dev proj && \
    apk add --update --no-cache imagemagick git && \
    apk add --update --no-cache rust cargo && \
    apk del .build-deps

RUN ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip
RUN pip3 install --no-cache --upgrade pip setuptools wheel

WORKDIR /code/
RUN git clone https://github.com/gkarthik/biothings_covid19.git

WORKDIR /code/biothings_covid19/
RUN pip3 install numpy	# https://github.com/pandas-dev/pandas/issues/25193
RUN pip3 install --no-cache-dir -r requirements.txt

RUN cargo install gifski
run Rscript install_requirements.R
