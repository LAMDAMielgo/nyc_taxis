# Custom DataFlow img to deploy from Airflow without download
FROM apache/beam_python3.9_sdk:2.47.0

# Make your customizations here, for example:
ENV PROJECT='graphite-bliss-388109'
ENV REGION='europe-southwest1'
ENV DATASETNAME='yellow_tripdata'

COPY ./requirements.txt requirements.txt

RUN pip install -r requirements.txt
RUN pip check

ENTRYPOINT ["/opt/apache/beam/boot"]