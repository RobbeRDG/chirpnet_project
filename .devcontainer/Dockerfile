FROM python:3.10-slim

# Set poetry environment variables
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=0 \
    POETRY_HOME=/opt/poetry \
    POETRY_VERSION=1.8.0

# Set bash as the shell env
ENV SHELL=/bin/bash

# Install packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="$POETRY_HOME/bin:$PATH"



