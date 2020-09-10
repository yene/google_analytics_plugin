"""

"""

import time
import os

from airflow.hooks.base_hook import BaseHook
from airflow import configuration as conf
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials
from collections import namedtuple


class GoogleAnalyticsHook(BaseHook):
    GAService = namedtuple('GAService', ['name', 'version', 'scopes'])
    # We need to rely on 2 services depending on the task at hand: reading from or writing to GA.
    _services = {
        'reporting': GAService(name='analyticsreporting',
                               version='v4',
                               scopes=['https://www.googleapis.com/auth/analytics.readonly']),
        'management': GAService(name='analytics',
                                version='v3',
                                scopes=['https://www.googleapis.com/auth/analytics'])
    }

    def __init__(self, key_file=None):
        self.file_location = key_file

    def get_service_object(self, name):
        service = GoogleAnalyticsHook._services[name]

        if hasattr(self, 'file_location'):
            credentials = ServiceAccountCredentials.from_json_keyfile_name(self.file_location, service.scopes)
        else:
            raise ValueError('No valid credentials could be found')

        return build(service.name, service.version, credentials=credentials)

    def get_management_report(self,
                              view_id,
                              since,
                              until,
                              metrics,
                              dimensions):

        analytics = self.get_service_object(name='management')

        return analytics.data().ga().get(
            ids=view_id,
            start_date=since,
            end_date=until,
            metrics=metrics,
            dimensions=dimensions).execute()

    def get_analytics_report(self,
                             view_id,
                             since,
                             until,
                             sampling_level,
                             dimensions,
                             metrics,
                             page_size,
                             include_empty_rows):

        analytics = self.get_service_object(name='reporting')

        reportRequest = {
            'viewId': view_id,
            'dateRanges': [{'startDate': since, 'endDate': until}],
            'samplingLevel': sampling_level or 'LARGE',
            'dimensions': dimensions,
            'metrics': metrics,
            'pageSize': page_size or 1000,
            'includeEmptyRows': include_empty_rows or False
        }

        response = (analytics
                    .reports()
                    .batchGet(body={'reportRequests': [reportRequest]})
                    .execute())

        if response.get('reports'):
            report = response['reports'][0]
            rows = report.get('data', {}).get('rows', [])

            while report.get('nextPageToken'):
                time.sleep(1)
                reportRequest.update({'pageToken': report['nextPageToken']})
                response = (analytics
                    .reports()
                    .batchGet(body={'reportRequests': [reportRequest]})
                    .execute())
                report = response['reports'][0]
                rows.extend(report.get('data', {}).get('rows', []))

            if report['data']:
                report['data']['rows'] = rows

            return report
        else:
            return {}


