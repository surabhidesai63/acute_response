FROM rocker/r-base:latest

# Install system dependencies
RUN apt-get update && apt-get install -y awscli
aws s3 ls s3://acute-response-bucket --region eu-north-1
