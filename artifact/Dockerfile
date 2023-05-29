
FROM apache/beam_python3.9_sdk:2.47.0

# copy local files
COPY ./extract /extract
COPY ./requirements.txt requirements.txt
COPY ./setup.py ./setup.py

RUN pip install -r requirements.txt
RUN pip check

ENTRYPOINT ["/opt/apache/beam/boot"]
