# Base image
FROM python:3.10

# Set working directory to the pipeline directory
WORKDIR /home/runner/work/data_pipeline/data_pipeline

# Copy all files from current directory to working dir in docker image
COPY . .

WORKDIR /home/runner/work/data_pipeline/data_pipeline/dune_nft_trades

# Install dependencies
RUN pip install -r requirements.txt

# CMD run.sh
CMD ["bash", "run.sh"]