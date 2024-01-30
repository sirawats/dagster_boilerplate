# Build stage
FROM python:3.11-slim AS builder

# install PDM
RUN pip install -U pip setuptools wheel
RUN pip install pdm

# copy files
COPY pyproject.toml pdm.lock README.md /project/
COPY src/ /project/src

# install dependencies and project into the local packages directory
WORKDIR /project
RUN mkdir __pypackages__ && pdm sync --prod --no-editable

# ------------------------------------------------------------------------------

# Run stage
FROM python:3.11-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

RUN pip install dagster-webserver dagster-postgres dagster-aws

COPY deployment/workspace.yaml /opt/dagster/app/

# Copy your code and workspace to /opt/dagster/app
COPY --from=builder /project/src/data_pipeline /opt/dagster/app/data_pipeline
COPY --from=builder /project/src/dagster_entry /opt/dagster/app/dagster_entry

# retrieve packages from build stage
ENV PYTHONPATH=/project/pkgs
COPY --from=builder /project/__pypackages__/3.11/lib /project/pkgs
# retrieve executable
COPY --from=builder /project/__pypackages__/3.11/bin/* /bin/

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

# Copy dagster instance YAML to $DAGSTER_HOME
COPY ./deployment/dagster.yaml /opt/dagster/dagster_home/

WORKDIR /opt/dagster/app

# RUN playwright install
# RUN playwright install-deps

EXPOSE 3000

ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
