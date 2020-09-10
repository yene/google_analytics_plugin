# Airflow Plugin - Google analytics

## Example

```bash
git clone https://github.com/yene/google_analytics_plugin.git $AIRFLOW_HOME/plugins/google_analytics_plugin
```

```python
from airflow.hooks.google_analytics_plugin import GoogleAnalyticsHook

google_analytics = GoogleAnalyticsHook(
    key_file='/mnt/secrets/google-api-key.json',
)

viewID = '12321312321'

start_date = '2020-09-09'
end_date = 'today'

a = google_analytics.get_management_report(
    'ga:' + viewID,
    start_date,
    end_date,
    'ga:users',
    'ga:dateHourMinute,ga:dimension1,ga:channelGrouping,ga:fullReferrer,ga:campaign,ga:adMatchedQuery,ga:keyword'
)
```
