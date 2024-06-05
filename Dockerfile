FROM python:3.11-slim
WORKDIR /code

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PYTHONDONTWRITEBYTECODE=1 \
  # pip:
  PIP_NO_CACHE_DIR=1 \
  PIP_DISABLE_PIP_VERSION_CHECK=1 \
  PIP_DEFAULT_TIMEOUT=100 \
  PIP_ROOT_USER_ACTION=ignore \
  # poetry:
  POETRY_NO_INTERACTION=1 \
  POETRY_VIRTUALENVS_CREATE=false \
  POETRY_CACHE_DIR='/var/cache/pypoetry' \
  POETRY_HOME='/usr/local' \
  # internal settings:
  SERVER_PORT=8080

SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

RUN apt update && apt upgrade -y \
    && apt install -y --no-install-recommends \
    bash \
    build-essential \
    curl \
    git \
    wait-for-it \
    && curl -sSL 'https://install.python-poetry.org' | python - \
    && poetry --version \
    && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /code/

RUN --mount=type=cache,target="$POETRY_CACHE_DIR" echo "production" \
  && poetry version \
  # Install deps:
  && poetry run pip install -U pip \
  && poetry install --no-interaction --no-ansi --sync

COPY ./run.py /code/run.py
COPY ./settings.py /code/settings.py
COPY ./judges /code/judges
COPY ./app /code/app

RUN mkdir -p /usr/share/baca2_share/packages

EXPOSE 8080
CMD ["poetry", "run", "python3", "./run.py"]
