FROM python:3.9
WORKDIR /code

#COPY ./requirements.txt /code/requirements.txt
#
#RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./run.py /code/run.py
COPY ./settings.py /code/settings.py
COPY ./judges /code/judges
COPY ./app /code/app

CMD ["python3", "./run.py"]
