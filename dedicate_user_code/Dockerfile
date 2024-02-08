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

RUN pip install dagster 

# [To Edit]
# Copy your code and workspace to /opt/dagster/app
COPY --from=builder /project/src/my_pipeline /opt/dagster/app/my_pipeline

# retrieve packages from build stage
ENV PYTHONPATH=/project/pkgs
COPY --from=builder /project/__pypackages__/3.11/lib /project/pkgs
# retrieve executable
COPY --from=builder /project/__pypackages__/3.11/bin/* /bin/

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

WORKDIR /opt/dagster/app

# RUN playwright install
# RUN playwright install-deps

EXPOSE 4200

ENTRYPOINT ["dagster", "code-server", "start", "-h","0.0.0.0", "-p", "4200", "-m", "my_pipeline"]
