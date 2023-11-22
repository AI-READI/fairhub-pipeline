FROM python:3.8-alpine

EXPOSE 5000

WORKDIR /app

ENV POETRY_VERSION=1.3.2

RUN pip install "poetry==$POETRY_VERSION"

COPY poetry.lock pyproject.toml ./

RUN poetry config virtualenvs.create false
RUN poetry install

COPY apis ./apis
COPY core ./core
COPY app.py .
COPY config.py .

COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]