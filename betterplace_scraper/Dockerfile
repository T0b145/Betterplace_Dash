FROM python:3.6-slim

RUN mkdir -p /home/project/app
WORKDIR /home/project/app

COPY betterplace.py /home/project/app/
#ADD assets /home/project/app/assets

COPY requirements.txt /home/project/app
RUN pip3 install --no-cache-dir -r requirements.txt

VOLUME /home/project/app/logs

#ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD ["python3", "betterplace.py", "-t"]
