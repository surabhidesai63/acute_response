FROM rocker/r-base:latest

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
RUN Rscript -e "install.packages(c('rentrez','here','aws.s3','rcrossref','lubridate','writexl','XML','xml2','stringr'), repos='https://cran.rstudio.com/')"

# RUN Rscript -e "if (!requireNamespace('tidyverse', quietly = TRUE)) stop('tidyverse not installed')"

# Clone the GitHub repository
RUN git clone https://github.com/surabhidesai63/acute_response.git /usr/src/app

# Set the working directory
WORKDIR /usr/src/app

# Copy the R script
# COPY code/3_parsing_xml.R /usr/src/app/3_parsing_xml.R

# CMD ["Rscript", "code/3_parsing_xml.R"]

# Copy the entire code directory
COPY code/ /usr/src/app/code/

# Run all R scripts in the code directory sequentially
# CMD ["bash", "-c", "for file in code/*.R; do echo Running $file; Rscript $file || exit 1; done"]
CMD ["bash", "-c", "for file in code/*.R; do echo Running $file; Rscript $file || exit 1; done; exit 0"]
