FROM python:3.12-slim

# Download the latest installer
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

# Create directory for the data
RUN mkdir data

# Bronze path
RUN mkdir data/raw

# Silver path
RUN mkdir data/silver

# Copy the project into the image
COPY . .

# Disable development dependencies
ENV UV_NO_DEV=1

# Sync the project into a new environment, asserting the lockfile is up to date
RUN uv --version
RUN uv sync --locked

# Presuming there is a `my_app` command provided by the project
CMD ["uv", "run", "main.py"]