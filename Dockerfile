FROM rocker/r-ver:4.3.0

# Install system dependencies
RUN apt-get update -qq && apt-get install -y \
    git \
      libxml2-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    && apt-get clean

# Install tidyverse with verbose output
RUN Rscript -e "install.packages('tidyverse', repos='https://cran.rstudio.com/', dependencies = TRUE)"

# Install additional R packages
RUN Rscript -e "install.packages(c('rentrez','here','aws.s3'), repos='https://cran.rstudio.com/')"

RUN Rscript -e "if (!requireNamespace('tidyverse', quietly = TRUE)) stop('tidyverse not installed')"

RUN Rscript -e "install.packages(c('rcrossref','lubridate','writexl','XML','xml2','aws.s3'), repos='https://cran.rstudio.com/')"

# Clone the GitHub repository
RUN git clone https://github.com/surabhidesai63/acute_response.git /usr/src/app

# Set the working directory
WORKDIR /usr/src/app

# Copy the R script
COPY code/3_parsing_xml.R /usr/src/app/3_parsing_xml.R

# Command to run all R scripts in queue
# CMD ["bash", "-c", "for file in *.R; do echo Running $file; Rscript $file || exit 1; done"]
CMD ["Rscript", "code/3_parsing_xml.R"]
