FROM python:3.7-slim

COPY . /src/
WORKDIR /src/

RUN pip install --upgrade pip
RUN pip install .
RUN pip install pytest

ENTRYPOINT [ "python", "-m", "pytest" ]