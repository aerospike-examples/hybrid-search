FROM python:3.11

RUN mkdir /server /model
COPY ./server /server
WORKDIR /server
RUN python3 -m pip install -r requirements.txt && \
    playwright install-deps && \
    playwright install chromium

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]