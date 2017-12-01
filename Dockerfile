FROM python:3.6.3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY email-database-importer.py ./

CMD [ "python", "./email-database-importer.py" ]