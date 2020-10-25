FROM python:3.6-slim

RUN mkdir -p /home/project/app
WORKDIR /home/project/app

COPY app.py /home/project/app/
ADD assets /home/project/app/assets

COPY requirements.txt /home/project/app

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 8910

#ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD ["gunicorn", "--error-logfile", "/home/project/app/error.log", "--access-logfile", "/home/project/app/access.log", "--capture-output", "--log-level", "debug", "--timeout", "120", "--preload" ,"--bind", "0.0.0.0:8910", "app:server"]
