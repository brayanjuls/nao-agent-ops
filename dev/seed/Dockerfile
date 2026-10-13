FROM docker.io/library/python:3.12-slim
RUN pip install --no-cache-dir boto3 "deltalake>=1" pyarrow
COPY seed.py /seed.py
ENTRYPOINT ["python", "/seed.py"]
