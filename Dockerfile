FROM python:3.7

RUN pip install kopf kubernetes

COPY handler.py /handler.py

CMD kopf run --standalone /handler.py
