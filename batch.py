import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

# general informations regarding the project & the Schema for BigQuery
PROJECT_ID = 'sandbox-mscherding'
SCHEMA = 'show_id:INTEGER,type:STRING,title:STRING,director:STRING,country:STRING,release_year:FLOAT,rating:FLOAT,duration:FLOAT'

# filters out records that don't have an information
def discard_incomplete(data):
    return len(data['show_id']) > 0 and len(data['type']) > 0 and len(data['title']) > 0 and len(data['director']) > 0 and len(data['country']) > 0 and len(data['release_year']) > 0 and len(data['rating']) > 0 and len(data['duration']) > 0

# converts string values to their appropriate type
def convert_types(data):
    data['show_id'] = int(data['show_id']) if 'show_id' in data else None
    data['type'] = str(data['type']) if 'type' in data else None
    data['title'] = str(data['title']) if 'title' in data else None
    data['director'] = str(data['director']) if 'director' in data else None
    data['country'] = str(data['country']) if 'country' in data else None
    data['release_year'] = float(data['release_year']) if 'release_year' in data else None
    data['rating'] = float(data['rating']) if 'rating' in data else None
    data['duration'] = float(data['duration']) if 'duration' in data else None
    return data

# delete the unwanted columns
def del_unwanted_cols(data):
    del data['cast']
    del data['date_added']
    del data['listed_in']
    del data['description']
    return data

# main for function call
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    with beam.Pipeline(options=PipelineOptions()) as pipeline:

    (pipeline 
     | 'ReadData' >> beam.io.ReadFromText('gs://nf-bucket-test/batch/netflix_titles.csv', skip_header_lines =1)
     | 'SplitData' >> beam.Map(lambda x: x.split(','))
     | 'FormatToDict' >> beam.Map(lambda x: {"show_id": x[0], "type": x[1], "title": x[2], "director": x[3], "country": x[4], "release_year": x[5], "rating": x[6], "duration": x[7]}) 
     | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
     | 'ChangeDataType' >> beam.Map(convert_types)
     | 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
     | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
         '{0}:nf_dataset.nf_data'.format(PROJECT_ID),
         schema=SCHEMA,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = pipeline.run()
    result.wait_until_finish()
