FROM ubuntu:18.04
MAINTAINER Karthik <gkarthik@scripps.edu>

RUN apt-get update
ENV DEBIAN_FRONTEND="noninteractive"
RUN apt-get install -y build-essential autoconf zlib1g-dev python3 wget libbz2-dev liblzma-dev libncurses-dev git python3-pip vim software-properties-common

# R version >= 3.5 for raster
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/'
RUN apt-get update && apt-get install -y r-base

# Dependencies for R packages
RUN add-apt-repository ppa:ubuntugis/ppa && apt-get update
RUN apt-get update
RUN apt-get install -y gdal-bin
RUN add-apt-repository -y ppa:cran/poppler && apt-get update
RUN apt-get install -y libfontconfig1-dev libgit2-dev libssl-dev libssh2-1-dev libxml2-dev libwebp-dev cargo librsvg2-dev libudunits2-dev libgdal-dev libgeos-dev libproj-dev libcurl4-gnutls-dev libtesseract-dev unixodbc-dev libgeos-dev tesseract-ocr-eng libavfilter-dev libpoppler-cpp-dev libpq-dev libmagick++-dev

WORKDIR /code/
RUN git clone https://github.com/gkarthik/biothings_covid19.git

WORKDIR /code/biothings_covid19/
RUN pip3 install --no-cache-dir -r requirements.txt

RUN Rscript install_requirements.R

# Install jq to extract csv from json
RUN apt-get install -y jq

# Add docker config
COPY docker_config.ini /code/biothings_covid19/config.ini

# ImageMagick policy increase disk usage
COPY policy.xml /etc/ImageMagick-6/policy.xml

RUN apt-get install -y time

CMD time python3 preprocess.py > /logs/$(date +%Y-%m-%d).log 2>&1 && \
    time ./extract_csv.sh 2>> /logs/$(date +%Y-%m-%d).log && \
    time Rscript choropleth_gifs.R >> /logs/$(date +%Y-%m-%d).log 2>&1 && \
    time python3 add_breaks_to_json.py >> /logs/$(date +%Y-%m-%d).log 2>&1
