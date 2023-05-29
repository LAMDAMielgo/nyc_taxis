
# 3.9 instead of 3.10 beacuse of error
FROM apache/beam_python3.9_sdk:2.47.0

# copy local files
COPY ./artifact/src /src
COPY ./requirements.txt requirements.txt
COPY ./artifact/setup.py ./setup.py

RUN pip install -r requirements.txt
RUN pip check

ENTRYPOINT ["/opt/apache/beam/boot"]

