import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'sandbox-mscherding'
SCHEMA = 'sr:INTEGER,abv:FLOAT,id:INTEGER,name:STRING,style:STRING,ounces:FLOAT'

def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['show_id']) > 0 and len(data['type']) > 0 and len(data['title']) > 0 and len(data['director']) > 0 and len(data['cast']) > 0 and len(data['country']) > 0 and len(data['date_added']) > 0 and len(data['release_year']) > 0 and len(data['rating']) > 0 and len(data['duration']) > 0 and len(data['listed_in']) > 0 and len(data['description']) > 0

def convert_types(data):
    """Converts string values to their appropriate type."""
    data['abv'] = float(data['abv']) if 'abv' in data else None
    data['id'] = int(data['id']) if 'id' in data else None
    data['name'] = str(data['name']) if 'name' in data else None
    data['style'] = str(data['style']) if 'style' in data else None
    data['ounces'] = float(data['ounces']) if 'ounces' in data else None
    return data

def del_unwanted_cols(data):
    """Delete the unwanted columns"""
    del data['ibu']
    del data['brewery_id']
    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://nf-bucket-test/batch/netflix_titles.csv', skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"sr": x[0], "abv": x[1], "ibu": x[2], "id": x[3], "name": x[4], "style": x[5], "brewery_id": x[6], "ounces": x[7]}) 
       | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:nf.nf_data'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()
