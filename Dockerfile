FROM python:3.9
WORKDIR /app
ADD *.py ./
ADD requirements.txt .
ADD *.sh ./
RUN pip install --upgrade pip
RUN pip install -r ./requirements.txt
CMD ["./run.sh", "/tdata/master.json.gz", "/tmp/output"]
