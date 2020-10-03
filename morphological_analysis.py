#!/usr/bin/env python3

import apache_beam as beam
import csv

def get_filename_newest_daily(bucket_name, file_type, tdate, delimiter=None):
    from google.cloud import storage
    storage_client = storage.Client()
    prefixCondition = file_type + "/" + file_type + "_" + tdate + "_"
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefixCondition, delimiter=delimiter
    )
    from datetime import datetime as dt
    filename_list = {}
    for blob in blobs:
        file_date_str = blob.name.split('/')[-1].split('_')[-1].split('.')[0]
        tdatetime = dt.strptime(file_date_str, '%Y%m%d%H%M%S')
        filename_list[blob.name] = tdatetime
    maxResult = max(filename_list, key=filename_list.get)
    print(maxResult)
    return maxResult

def replace_double_quotes_in_line(line):
    return line.replace('"', '')

def is_contents(fields, mark):
    return fields[0] != mark

def morphological_analysis(fields, index):
    import MeCab

    # neo_wakati = MeCab.Tagger('-Owakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd')
    neo_wakati = MeCab.Tagger('-Owakati')

    word = fields[index]
    neo_wakati_result = neo_wakati.parse(word).strip()

    outputFields = []
    for field in fields:
        outputFields.append(field)
    outputFields.append(neo_wakati_result)
    return outputFields

def str_to_date(fields, tdate):
    outputFields = []
    outputFields.append(tdate)
    for field in fields:
        outputFields.append(field)
    return outputFields

def create_row(header, fields):
    featdict = {}
    for name, value in zip(header, fields):
        featdict[name] = value
    return featdict

def run(project, dataset, storagebucket, workbucket, tdate):
    argv = [
        # xxxxxxx for DataflowRunner xxxxxxx
        '--runner=DataflowRunner',
        '--job_name=ma{0}'.format(tdate),
        '--project={0}'.format(project),
        '--staging_location=gs://{0}/dataflow/staging/'.format(workbucket),
        '--temp_location=gs://{0}/dataflow/temp/'.format(workbucket),
        '--region=asia-northeast1',
        '--save_main_session',
        '--max_num_workers=4',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--setup_file=/var/dataflow/morphological_analysis/setup.py',
        # xxxxxxx for DataflowRunner xxxxxxx
        # ===== for DirectRunner ==========
        # '--runner=DirectRunner',
        # ===== for DirectRunner ==========
    ]

    with beam.Pipeline(argv=argv) as pipeline:
        # -------------------------------------------------------------------------------------------------------
        # ------------- daily_data start ----------------
        daily_data_path = 'gs://{}/'.format(storagebucket) + get_filename_newest_daily(storagebucket, 'daily_data', tdate)

        index_daily_data = 1
        mark_daily_data = "idx_id"
        header_daily_data = 'tdate,idx_id,text'.split(',')
        output_daily_data = '{}:{}.daily_data'.format(project, dataset)
        schema_daily_data = 'tdate:date,idx_id:string,text:string,wakati:string'
        
        daily_data = (pipeline
            | 'daily_data:read' >> beam.io.ReadFromText(daily_data_path)
            | 'daily_data:escape' >> beam.Map(replace_double_quotes_in_line)
            | 'daily_data:fields' >> beam.Map(lambda line: next(csv.reader([line], delimiter="\t")))
            | 'daily_data:morph' >> beam.Map(lambda fields: morphological_analysis(fields, index_daily_data))
            | 'daily_data:filter_header' >> beam.Filter(is_contents, mark=mark_daily_data)
            | 'daily_data:add_tdate' >> beam.Map(str_to_date, tdate='{0}-{1}-{2}'.format(tdate[0:4], tdate[4:6], tdate[6:8]))
            | 'daily_data:totablerow' >> beam.Map(lambda fields: create_row(header_daily_data, fields))
        )
        errors_daily_data = (daily_data
            | 'daily_data:out' >> beam.io.WriteToBigQuery(
                output_daily_data,
                schema=schema_daily_data,
                insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )
        # ===== for DirectRunner ==========
        # result_daily_data = (errors_daily_data
        #     # | 'daily_data:PrintErrors' >> beam.FlatMap(lambda err: print("[daily_data]Error Found {}".format(err)))
        #     | 'daily_data_error:PrintErrors' >> beam.io.WriteToText('daily_data_error')
        # )
        # ===== for DirectRunner ==========
        
        # ------------- daily_data end ----------------
        # -----------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument(
        '-p',
        '--project',
        help='Unique project ID',
        required=True
    )
    parser.add_argument(
        '-s', '--storagebucket',
        help='Google Cloud Storage Data Bucket Name',
        required=True
    )
    parser.add_argument(
        '-w', '--workbucket',
        help='Google Cloud Storage Work(tmp) Bucket Name',
        required=True
    )
    parser.add_argument(
        '-t',
        '--tdate',
        help='TargetDate（ex. 20200710）',
        required=True
    )
    parser.add_argument(
        '-d',
        '--dataset',
        help='BigQuery dataset',
        default='daily_files'
    )
    args = vars(parser.parse_args())

    run(project=args['project'], dataset=args['dataset'], storagebucket=args['storagebucket'], workbucket=args['workbucket'], tdate=args['tdate'])