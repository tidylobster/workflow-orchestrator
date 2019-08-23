FROM python:3.7-slim

COPY . /src/
WORKDIR /src/

RUN pip install --upgrade pip
RUN pip install --editable .
RUN pip install pytest

ENTRYPOINT [ "python", "-m", "pytest" ]