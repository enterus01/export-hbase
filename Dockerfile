FROM python:3.9

WORKDIR /code/app/

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . /code/app

CMD ["uvicorn", "app_api:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "8","--timeout-keep-alive","60" ]