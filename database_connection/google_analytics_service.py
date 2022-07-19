import numpy as np
import pandas as pd
import json
from google.oauth2 import service_account
from apiclient import discovery


class GoogleAnalyticsService:

    def __init__(self, credentials_file):
        self.credentials_file = credentials_file
        self.service = self.create_service()

    def create_service(self):
        # Create service credentials
        credentials = service_account.Credentials.from_service_account_file(self.credentials_file,
                                                                            scopes=[
                                                                                'https://www.googleapis.com/auth/analytics.readonly'])
        # Create a service object
        service = discovery.build('analyticsreporting', 'v4', credentials=credentials)
        return service

    def ga_api_caller(self, query):

        if query['reportRequests'].__len__() == 1:
            response_raw_data = self.service.reports().batchGet(body=query).execute()

            response_data = response_raw_data['reports'][0].get('data', [])
            if not response_data:
                print("No data was returned")
                return pd.DataFrame()

            next_page_token = response_raw_data['reports'][0].get('nextPageToken', False)

            while next_page_token:
                query['reportRequests'][0]['pageToken'] = next_page_token
                next_page_response_raw_data = self.service.reports().batchGet(body=query).execute()
                response_data['rows'].extend(next_page_response_raw_data['reports'][0]['data'].get('rows', []))
                next_page_token = next_page_response_raw_data['reports'][0].get('nextPageToken', False)
        else:
            e_mess = "This only works for single queries, add functionality for multiple queries if thats what you like"
            raise TypeError(e_mess)

        response_raw_data['reports'][0]['data'] = response_data

        if 'pivotValueRegions' in response_raw_data['reports'][0]['data']['rows'][0]['metrics'][0]:
            df_pivot = self.pivot_report_df(response=response_raw_data)
        else:
            df_pivot = pd.DataFrame()

        if 'metricHeader' in response_raw_data['reports'][0]['columnHeader']:
            df_metric = self.metric_report_df(response=response_raw_data)
        else:
            df_metric = pd.DataFrame()

        if df_pivot.columns.nlevels == 2:
            df_metric.columns = [[''] * len(df_metric.columns), df_metric.columns]

        df_report = (pd.concat([df_metric, df_pivot], axis=1))
        return df_report

    def metric_report_df(self, response):
        # extract column names
        summary_column_names = [item['name'] for item in response['reports'][0]
        ['columnHeader']['metricHeader']['metricHeaderEntries']]

        # extract table values
        summary_values = [element['metrics'][0]['values'] for element in response['reports'][0]['data']['rows']]

        # Use dimension names to get rows
        row_index_named = self.get_dimension_row_names(response=response)

        # combine. I used type 'float' because default is object, and as far as I know, all values are numeric
        return pd.DataFrame(data=np.array(summary_values),
                            index=row_index_named,
                            columns=summary_column_names).astype('float')

    def pivot_report_df(self, response):
        # extract table values
        pivot_values = [item['metrics'][0]['pivotValueRegions'][0]['values'] for item in
                        response['reports'][0]['data']['rows']]

        # create column index
        top_header = [item['dimensionValues'] for item in
                      response['reports'][0]['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        column_metrics = [item['metric']['name'] for item in
                          response['reports'][0]['columnHeader']['metricHeader']['pivotHeaders'][0][
                              'pivotHeaderEntries']]
        array = np.concatenate((np.array(top_header),
                                np.array(column_metrics).reshape((len(column_metrics), 1))),
                               axis=1)
        column_index = pd.MultiIndex.from_arrays(np.transpose(array))

        row_index_named = self.get_dimension_row_names(response=response)

        return pd.DataFrame(data=np.array(pivot_values),
                            index=row_index_named,
                            columns=column_index).astype('float')

    @staticmethod
    def get_dimension_row_names(response):
        if 'dimensions' in response['reports'][0]['columnHeader']:
            row_index_names = response['reports'][0]['columnHeader']['dimensions']
            row_index = [element['dimensions'] for element in response['reports'][0]['data']['rows']]
            row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)),
                                                        names=np.array(row_index_names))
        else:
            row_index_named = None
        return row_index_named


