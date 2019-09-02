import os
from zipfile import ZipFile
import asyncio
import requests
import isodate as iso
import json
import pandas as pd
import numpy as np
try:
    from pandas.tseries.resample import TimeGrouper
except:
    from pandas.core.resample import TimeGrouper
from datetime import datetime, timedelta
import pytz
import calendar
from telegram.ext import Updater, CommandHandler
from telegram.ext import MessageHandler, Filters, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from lxml import etree
import csv
from io import StringIO
from statistics import stdev
from tinydb import TinyDB, Query


class GenReport():
    def __init__(self, conf, logger=None):
        if logger is None:
            # SET LOGGER
            # Enable logging
            import logging
            LOG_INFO = os.path.join(
                os.path.dirname(__file__),
                'report.log'
            )

            LOG_ERROR = os.path.join(
                os.path.dirname(__file__),
                'report_error.log'
            )

            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)

            LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

            self.logger = logging.getLogger(__name__)
            log_formatter = logging.Formatter(LOG_FORMAT)

            # comment this to suppress console output
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(log_formatter)
            self.logger.addHandler(stream_handler)

            file_handler_info = logging.FileHandler(LOG_INFO, mode='a')
            file_handler_info.setFormatter(log_formatter)
            file_handler_info.setLevel(logging.INFO)
            self.logger.addHandler(file_handler_info)

            file_handler_error = logging.FileHandler(LOG_ERROR, mode='a')
            file_handler_error.setFormatter(log_formatter)
            file_handler_error.setLevel(logging.ERROR)
            self.logger.addHandler(file_handler_error)

            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger
        # set general variable

        self.report_type = 'NOAA'
        self.properties = conf['properties']
        self.variables = []
        self.thresholds = conf['thresholds']
        self.country = conf['country']
        self.telegram_token = conf['telegram_token']
        self.telegram_db = conf['telegram_db']
        self.zenodo_token = conf['zenodo_token']
        self.zenodo_url = 'https://zenodo.org'
        self.deposition_daily = {
            "metadata": {
                "upload_type": "dataset",
                "publication_date": datetime.now().isoformat(),
                "title": "4onse stations daily data",
                "creators": [
                    {
                        "name": "Strigaro, Daniele",
                        "affiliation": "SUPSI"
                    }
                ],
                "communities": [
                    {
                        "identifier": "4onse"
                    }
                ],
                "description": (
                    "This data archive contain daily data of all the 4onse " +
                    "stations installed in the Deduru Oya basin in " +
                    "Sri Lanka. It is automatically daily updated."
                ),
                "access_right": "open",
                "license": "cc-by-sa",
                "grants": [
                    {
                        'id': '10.13039/501100001711::IZ07Z0_160906'
                    }
                ]
            }
        }
        self.deposition_monthly = {
            "metadata": {
                "upload_type": "dataset",
                "publication_date": datetime.now().isoformat(),
                "title": "4onse stations monthly reports",
                "creators": [
                    {
                        "name": "Strigaro, Daniele",
                        "affiliation": "SUPSI"
                    }
                ],
                "communities": [
                    {
                        "identifier": "4onse"
                    }
                ],
                "description": (
                    "This dataset contains monthly climatological reports " +
                    "(NOAA style) of all the 4onse stations installed " +
                    "in the Deduru Oya basin in Sri Lanka."
                ),
                "access_right": "open",
                "license": "cc-by-sa",
                "grants": [
                    {
                        'id': '10.13039/501100001711::IZ07Z0_160906'
                    }
                ]
            }
        }
        self.type = conf['type']
        self.base_path = os.path.dirname(__file__)

        # istsos parameters
        self.logger.info("***START***")
        self.logger.info("***CONFIGURING***")
        self.base_url = conf['base_url']
        self.basic_auth = (conf['basic_auth'][0], conf['basic_auth'][1])
        self.service = conf['service']
        self.dservice = conf['dservice']

        self.get_metadata()
        # plausible value
        self.plausible_value = conf['plausible_value']

        # set time variables
        today = datetime.today()
        self.today = today
        if 'year' in conf:
            self.year = conf['year']
        else:
            self.year = today.year
        if 'month' in conf:
            self.month = conf['month']
        else:
            self.month = today.month

        if 'day' in conf:
            self.day = conf['day']
        else:
            self.day = today.day-1
        if 'tz' in conf:
            self.tz = conf['tz']
        else:
            self.tz = str(pytz.utc)

        self.get_event_time()

        if self.type == 'daily':
            self.logger.info(
                f"***START_DAILY_REPORT {self.day}/{self.month}/{self.year}***"
            )
            # get_observations
            self.logger.info("***GET_OBSERVATIONS***")
            self.get_observations()

            # quality check
            self.logger.info("***QUALITY_CHECK***")
            self.quality_check()

            # saving files
            self.logger.info("***DAILY_REPORT***")
            self.save_report()
        else:
            self.logger.info(
                f"***START_MONTHLY_REPORT {self.month}/{self.year}***"
            )
            # get_observations
            self.logger.info("***GET_OBSERVATIONS***")
            self.get_observations()
            # statistics
            self.logger.info("***MONTHLY_STATISTICS***")
            self.get_stats()

    def get_observations(self):
        for station in self.stations:
            obs = list(
                map(
                    lambda x: x['name'], station['observed_property']
                )
            )

            url = (
                "{}{}?service=SOS&request=GetObservation&version=1.0.0&" +
                "offering=temporary&procedure={}&eventTime={}" +
                "&observedProperty={}&responseFormat=text/plain&" +
                "qualityIndex=True").format(
                self.base_url,
                self.dservice if self.type == 'monthly' else self.service,
                station['name'].split(':')[-1],
                self.event_time,
                ','.join(obs)
            )
            r = requests.get(
                url,
                auth=self.basic_auth
            )
            f = StringIO(r.text)
            r.close()
            df = pd.read_csv(
                f,
                delimiter=',',
                header=0,
                index_col=0,
                parse_dates=[0]
            )
            if self.type == 'monthly':
                station['df'] = df[:self.end_time-timedelta(days=1)]
            else:
                station['df'] = df[:self.end_time]

    def get_event_time(self):
        if self.type == 'monthly':
            self.begin_time = pytz.timezone(self.tz).localize(
                datetime(
                    self.year,
                    self.month,
                    1
                )
            )

            self.end_time = pytz.timezone(self.tz).localize(
                datetime(
                    self.year if self.month < 12 else self.year + 1,
                    self.month+1 if self.month < 12 else 1,
                    1
                )
            )
        elif self.type == 'daily':
            self.begin_time = pytz.timezone(self.tz).localize(
                datetime(
                    self.year,
                    self.month,
                    self.day,
                    23
                )
            ) - timedelta(days=1)
            self.end_time = pytz.timezone(self.tz).localize(
                datetime(
                    self.year,
                    self.month,
                    self.day,
                    1
                )
            ) + timedelta(days=1)
        self.event_time = '{}/{}'.format(
            self.begin_time.isoformat(),
            self.end_time.isoformat()
        )
        self.month_name = self.begin_time.strftime("%B")

    def get_metadata(self):
        url = "{}{}{}".format(
            self.base_url,
            self.dservice if self.type == 'monthly' else self.service,
            '?service=SOS&request=GetCapabilities&acceptversions=2.0.0'
        )
        d_url = "{}{}{}".format(
            self.base_url,
            self.dservice if self.type == 'monthly' else self.service,
            (
                "?service=SOS&request=DescribeSensor&version=2.0.0&" +
                "procedureDescriptionFormat=" +
                "http://www.opengis.net/sensorML/1.0.1&procedure="
            )
        )
        r = requests.get(
            url,
            auth=self.basic_auth
        )
        gc_text = r.text
        r.close()
        gc = etree.fromstring(
            gc_text.encode('utf-8')
        )
        om = gc.find(
            "{http://www.opengis.net/ows/1.1}OperationsMetadata"
        )
        omo = om.findall(
            "{http://www.opengis.net/ows/1.1}Operation"
        )
        self.stations = []
        for i in omo:
            if i.attrib['name'] == 'DescribeSensor':
                p = i.find(
                    "{http://www.opengis.net/ows/1.1}Parameter"
                )
                pavs = p.find(
                    "{http://www.opengis.net/ows/1.1}AllowedValues"
                )
                for pav in pavs:
                    name = pav.text
                    if ('test' not in name.lower() and
                        'template' not in name.lower() and
                        'xl' not in name.lower() and
                        'hi_' not in name.lower() and
                            'spi_' not in name.lower()):
                        r = requests.get(
                            d_url+name,
                            auth=self.basic_auth
                        )
                        r_text = r.text
                        r.close()
                        ds = etree.fromstring(
                            r_text.encode('utf-8')
                        )
                        outs = ds[1][0][0][0][0][0].find(
                            "{http://www.opengis.net/sensorML/1.0.1}outputs"
                        )[0][0][0].findall(
                            "{http://www.opengis.net/swe/1.0.1}field"
                        )

                        obs = list(map(
                            lambda x: {
                                'name': x[0].attrib['definition'],
                                'uom': x[0][0].attrib['code']
                            }, outs[1:]
                        ))

                        location = outs = ds[1][0][0][0][0][0].find(
                            "{http://www.opengis.net/sensorML/1.0.1}location"
                        )
                        self.stations.append(
                            {
                                'name': name,
                                'location': location[0].attrib[
                                    '{http://www.opengis.net/gml}id'
                                ],
                                'observed_property': obs,
                                'coordinates': location[0][0].text,
                                'srs_name': location[0].attrib['srsName'],
                                'sos_ds': r_text,
                                'daily_stats': None,
                            }
                        )

    def quality_check(self):
        for station in self.stations:
            df_temp = station['df'].copy()
            df_temp = df_temp.replace(-100, 400)
            df_temp = df_temp.replace(40, 400)
            df_temp = df_temp.replace('None', -999.99)
            for ob in station['observed_property']:
                if ob['name'] in self.plausible_value.keys():

                    # check plausible value
                    if 'p' in self.plausible_value[ob['name']]['methods']:
                        ts_check1 = df_temp.loc[
                                df_temp[ob['name']+':qualityIndex'] != 400
                            ][ob['name']].apply(
                            lambda x: self.plausible_value_check(
                                x,
                                ob
                            )
                        )
                        df_temp[ob['name']+':qualityIndex'].update(
                            ts_check1.where(lambda x: x > 0)
                        )
                        df_temp[ob['name']+':qualityIndex'].update(
                            ts_check1.where(lambda x: x == 0)
                        )

                        # check time consistency
                        if 't' in self.plausible_value[ob['name']]['methods']:
                            ts_check2 = df_temp.loc[
                                df_temp[ob['name']+':qualityIndex'] == 700
                            ].copy()
                            if not ts_check2.empty:
                                ts_check2['T'] = ts_check2.index
                                ts_check2['q'] = np.nan
                                ts_time = ts_check2.copy()
                                ts_time.apply(
                                    lambda y: self.time_consistency_check(
                                        y,
                                        ob['name'],
                                        ts_check2
                                    ),
                                    axis=1
                                )
                                # ts_check2 = ts_check2[ob['name']].rolling(
                                # '1320s').apply(
                                # lambda x:
                                # self.time_consistency_check(x), raw=True)
                                df_temp[ob['name']+':qualityIndex'].update(
                                    ts_check2['q'].where(lambda x: x > 0)
                                )

                            # check minimum variability
                            if 'm' in self.plausible_value[
                                    ob['name']]['methods']:
                                ts_check3 = df_temp.loc[
                                    df_temp[ob['name']+':qualityIndex'] == 705
                                ]
                                ts_check3 = ts_check3[
                                    ob['name']
                                ].rolling('7200s').apply(
                                    lambda x: self.minimum_variability_check(
                                        x,
                                        ob
                                    ), raw=True
                                )
                                df_temp[ob['name']+':qualityIndex'].update(
                                    ts_check3.where(lambda x: x > 0)
                                )

            # if 'WEWALA_PCB' in station['name']:
            #     print(df_temp)
            station['df_checked'] = df_temp

    def save_report(self):
        if self.type == 'daily':
            directory = os.path.join(self.base_path, 'data', 'daily')
            if not os.path.exists(directory):
                os.makedirs(directory)
            for station in self.stations:
                station['df_checked'].to_csv(
                    os.path.join(
                        directory,
                        '{}_{}{}{}.txt'.format(
                            station['name'].split(':')[-1],
                            self.day if len(
                                str(self.day)) == 2 else f'0{self.day}',
                            self.month if len(
                                str(self.month)) == 2 else f'0{self.month}',
                            self.year
                        )
                    )
                )

    def plausible_value_check(self, x, ob_prop):
        col_lower = ob_prop['name']
        uom = ob_prop['uom']
        if col_lower in self.plausible_value.keys():
            if (float(x) >= self.plausible_value[col_lower][uom][0] and
                    float(x) <= self.plausible_value[col_lower][uom][1]):
                return 700
            else:
                return 0
        else:
            return 0

    def time_consistency_check(self, row, col, df):
        x = row['T']
        try:
            ts_values = df[col].loc[
                x-timedelta(seconds=660):x+timedelta(seconds=660)
            ]
        except Exception as e:
            msg = (
                "Error during %s filtering by time: %s" % (
                    row['urn:ogc:def:procedure'], str(e)
                )
            )
            self.logger.error(msg)
            raise e
        list_values = [float(v) for v in ts_values.values]
        ref_val = float(ts_values[x])
        idx_val_list = ts_values.index.to_list()
        idx_ref_val = idx_val_list.index(x)
        if len(ts_values) < 2:
            return False
        if idx_ref_val == 0:
            try:
                abs_val = [
                    abs(ref_val - list_values[idx_ref_val+1])
                ]
                std_ = 2*stdev(list_values)
            except Exception as e:
                msg = (
                    "Error during %s std calculation: %s" % (
                        row['urn:ogc:def:procedure'], str(e)
                    )
                )
                self.logger.error(msg)
                raise e
        else:
            if len(ts_values) == 2 or len(ts_values) == (idx_ref_val+1):
                abs_val = [abs(ref_val - list_values[idx_ref_val-1])]
                std_ = 2*stdev(
                    list_values[idx_ref_val-1:idx_ref_val+1]
                )
            else:
                abs_val = [
                    abs(ref_val - list_values[idx_ref_val+1]),
                    abs(ref_val - list_values[idx_ref_val-1])
                ]
                std_ = 4*stdev(
                    list_values[idx_ref_val-1:idx_ref_val+2]
                )
        # if ref_val == 30.65:
        #     print(list_values)
        sum_abs_val = sum(abs_val)
        if sum_abs_val <= std_:
            df.at[x, 'q'] = 705
            return 705
        else:
            df.at[x, 'q'] = 0
            return False

    def minimum_variability_check(self, x, ob_prop):
        if sum(x)/len(x) == x[0]:
            return False
        else:
            return 710

    def stat_apply(self, ts, stat, col):
        if ts.empty:
            return np.nan
        else:
            if stat == 'max':
                return ts[col].idxmax()
            else:
                return ts[col].idxmin()

    def get_stats(self):
        if self.type == 'monthly':
            for station in self.stations:
                daily_stats = None
                name = station['name'].split(':')[-1]

                if 'PCB' in name.upper():
                    prop = 'pcb'
                else:
                    prop = 'default'
                if not station['df'].empty:
                    for col in station['df'].columns:
                        if col in self.properties[prop]:
                            if col in self.plausible_value:
                                if 'm' in self.plausible_value[col]['methods']:
                                    tmp_ = station['df'].loc[
                                        station['df'][
                                            col+':qualityIndex'
                                        ] == 710
                                    ].copy()
                                elif 't' in self.plausible_value[col][
                                        'methods']:
                                    tmp_ = station['df'].loc[
                                        station['df'][
                                            col+':qualityIndex'
                                        ] == 705
                                    ].copy()
                                else:
                                    tmp_ = station['df'].loc[
                                        station['df'][
                                            col+':qualityIndex'
                                        ] == 700
                                    ].copy()
                                if tmp_.empty:
                                    tmp_ = station['df'].copy()
                                    tmp_[col] = np.nan
                                for met in self.properties[prop][col][
                                        'methods']:
                                    tmp = tmp_.copy()
                                    col_name = '{}_{}'.format(
                                        self.properties[prop][col]['name'],
                                        met.upper()
                                    )
                                    tmp['time'] = tmp.index
                                    col_q = col+':qualityIndex'

                                    aggregations = {
                                        col: [met, 'count'],
                                        col_q: met,
                                        'time': lambda x: self.stat_apply(
                                            tmp.loc[x.min():x.max()], met, col
                                        )
                                    }
                                    grouped = tmp.groupby(
                                        TimeGrouper(
                                            freq='D',
                                            closed='left',
                                            label='left'
                                        )
                                    ).agg(aggregations)

                                    grouped.columns = grouped.columns.droplevel()
                                    grouped.columns = [
                                        col_name,
                                        'count',
                                        'q_{}'.format(met),
                                        'time'
                                    ]
                                    tmp = grouped
                                    if met in ['max', 'min']:
                                        tmp.rename(
                                            inplace=True,
                                            columns={
                                                'data': col_name,
                                                'time': 'TIME_' + col_name
                                            }
                                        )
                                    else:
                                        tmp.rename(
                                            inplace=True,
                                            columns={
                                                'data': col_name,
                                                'count': '{}_COUNT'.format(
                                                    col_name
                                                )
                                            }
                                        )
                                    if daily_stats is not None:
                                        if met in ['max', 'min']:
                                            daily_stats = daily_stats.join(
                                                round(tmp[[col_name]], 1)
                                            ).join(
                                                tmp[['TIME_' + col_name]]
                                            )
                                        else:
                                            daily_stats = daily_stats.join(
                                                round(tmp[[col_name]], 1)
                                            ).join(
                                                tmp[[col_name+'_COUNT']]
                                            )
                                    else:
                                        daily_stats = tmp[[col_name]]
                                        if met in ['max', 'min']:
                                            daily_stats = daily_stats.join(
                                                tmp[['TIME_' + col_name]]
                                            )
                                        else:
                                            daily_stats = daily_stats.join(
                                                tmp[[col_name+'_COUNT']]
                                            )
                    station['daily_stats'] = daily_stats
                else:
                    station['daily_stats'] = daily_stats

    def create_spaces(self, num, len_field=None, val=None, char=None):
        output = ''
        for i in range(num):
            output = output + ' '
        if val is not None:
            if len(str(val)) < len_field:
                for i in range(len_field-len(str(val))):
                    output = output + ' '

            output = '{}{}'.format(output, val)
        if char is not None:
            output = output.replace(' ', char)
        return output

    def generate_str_row(self, data):
        row_text = (
            self.create_spaces(0, 2, data[0]) +
            self.create_spaces(3, 5, data[1]) +
            self.create_spaces(2, 5, data[2]) +
            self.create_spaces(2, 5, data[3]) +
            self.create_spaces(2, 5, data[4]) +
            self.create_spaces(2, 5, data[5]) +
            self.create_spaces(2, 6, data[6]) +
            self.create_spaces(2, 6, data[7]) +
            self.create_spaces(2, 5, data[8]) +
            self.create_spaces(2, 3, data[9]) +
            self.create_spaces(3, 4, data[10]) +
            self.create_spaces(2, 5, data[11]) +
            self.create_spaces(2, 5, data[12]) + '\n'
        )
        return row_text

    def export_report(self):
        directory = os.path.join(self.base_path, 'data', 'monthly')
        if not os.path.exists(directory):
            os.makedirs(directory)
        for station in self.stations:
            if station['daily_stats'] is not None:
                if 'MOD' in station['name'] or 'PCB' in station['name']:
                    data = []
                    output_txt = ''
                    stat_name = station['name'].split(':')[-1]

                    # max mean and min values of the month
                    t_highest = round(
                        station['daily_stats']['TEMP_MAX'].max(), 1
                    )
                    try:
                        t_max_idx = station['daily_stats'][
                            'TEMP_MAX'
                        ].idxmax().strftime('%d')
                    except:
                        t_max_idx = station['daily_stats'][
                            'TEMP_MAX'
                        ].idxmax()
                    t_lowest = round(station['daily_stats'][
                        'TEMP_MIN'
                    ].min(), 1)
                    try:
                        t_min_idx = station['daily_stats'][
                            'TEMP_MIN'
                        ].idxmin().strftime('%d')
                    except:
                        t_min_idx = station['daily_stats']['TEMP_MIN'].idxmin()
                    t_avg = round(station['daily_stats'][
                        'TEMP_MEAN'
                    ].mean(), 1)
                    station['daily_stats']['RAIN_SUM']
                    r_sum = round(station['daily_stats']['RAIN_SUM'].sum(), 1)
                    p_avg = round(station['daily_stats'][
                        'PRESS_MEAN'
                    ].mean(), 1)
                    if 'PCB' in stat_name:
                        p_avg = p_avg*10.0
                    h_avg = round(station['daily_stats']['HUM_MEAN'].mean(), 1)
                    ws_avg = round(station['daily_stats'][
                        'WINDSPEED_MEAN'
                    ].mean(), 1)
                    ws_highest = round(station['daily_stats'][
                        'WINDSPEED_MAX'
                    ].max(), 1)
                    try:
                        ws_max_idx = station['daily_stats'][
                            'WINDSPEED_MAX'
                        ].idxmax().strftime('%d')
                    except:
                        ws_max_idx = station['daily_stats'][
                            'WINDSPEED_MAX'
                        ].idxmax()
                    wd_avg = round(station['daily_stats'][
                        'WINDDIRECTION_MEAN'
                    ].mean(), 1)

                    # threshold based calculations
                    tmax_eh = station['daily_stats'][
                        'TEMP_MAX'
                    ] >= self.thresholds['tmax_eh']
                    tmax_eh = tmax_eh.sum()
                    tmax_el = station['daily_stats'][
                        'TEMP_MAX'
                    ] <= self.thresholds['tmax_el']
                    tmax_el = tmax_el.sum()
                    tmin1_el = station['daily_stats'][
                        'TEMP_MIN'
                    ] <= self.thresholds['tmin1_el']
                    tmin1_el = tmin1_el.sum()
                    tmin2_el = station['daily_stats'][
                        'TEMP_MIN'
                    ] <= self.thresholds['tmin2_el']
                    tmin2_el = tmin2_el.sum()
                    r1_eh = station['daily_stats'][
                        'RAIN_SUM'
                    ] >= self.thresholds['r1_eh']
                    r1_eh = r1_eh.sum()
                    r2_eh = station['daily_stats'][
                        'RAIN_SUM'
                    ] >= self.thresholds['r2_eh']
                    r2_eh = r2_eh.sum()
                    r3_eh = station['daily_stats'][
                        'RAIN_SUM'
                    ] >= self.thresholds['r3_eh']
                    r3_eh = r3_eh.sum()
                    r_max = round(station['daily_stats']['RAIN_SUM'].max(), 1)
                    r_max_idx = station['daily_stats'][
                        'RAIN_SUM'
                    ].idxmax().strftime('%d')

                    index_row = 1
                    for row in station['daily_stats'].iterrows():
                        day_n = row[0].strftime('%d')
                        t_mean = round(row[1]['TEMP_MEAN'], 1)
                        t_max = round(row[1]['TEMP_MAX'], 1)
                        try:
                            t_max_time = row[1][
                                'TIME_TEMP_MAX'
                            ].strftime('%H:%M')
                        except:
                            t_max_time = ''
                        t_min = round(row[1]['TEMP_MIN'], 1)
                        try:
                            t_min_time = row[1][
                                'TIME_TEMP_MIN'
                            ].strftime('%H:%M')
                        except:
                            t_min_time = ''

                        if row[1]['RAIN_SUM_COUNT'] == 0:
                            rain_sum = np.nan
                        else:
                            rain_sum = row[1]['RAIN_SUM']
                        p_mean = round(row[1]['PRESS_MEAN'], 1)
                        if 'PCB' in stat_name:
                            p_mean = p_mean*10.0
                        h_mean = round(row[1]['HUM_MEAN'], 1)
                        ws_mean = round(row[1]['WINDSPEED_MEAN'], 1)
                        ws_max = round(row[1]['WINDSPEED_MAX'], 1)
                        try:
                            ws_max_time = row[1][
                                'TIME_WINDSPEED_MAX'
                            ].strftime('%H:%M')
                        except:
                            ws_max_time = ''
                        wd_mean = round(row[1]['WINDDIRECTION_MEAN'], 1)
                        if row[1]['HUM_MEAN_COUNT'] >= 0:
                            count = int(row[1]['HUM_MEAN_COUNT'])
                        else:
                            count = int(row[1]['TEMP_MEAN_COUNT'])

                        data_list = [
                            day_n, t_mean,
                            t_max, t_max_time,
                            t_min, t_min_time,
                            rain_sum, p_mean,
                            h_mean, ws_mean,
                            ws_max, ws_max_time,
                            wd_mean
                        ]
                        data_list = list(
                            map(
                                lambda x: '' if str(x) == 'nan' else x,
                                data_list
                            )
                        )
                        data.append(data_list)
                        index_row += 1

                    head1 = (
                        '{}MONTHLY CLIMATOLOGICAL SUMMARY for {} {}' +
                        '\n\n\n'
                    ).format(
                        self.create_spaces(19),
                        self.month_name,
                        self.year
                    )
                    head2 = (
                        'NAME: {}         CITY: {}               ' +
                        'STATE: {}\n'
                    ).format(
                        stat_name,
                        station['location'],
                        self.country
                    )
                    coords = station['coordinates'].split(',')
                    head3 = (
                        'ELEV:  {} m    LAT: {} N    LONG: {} E' +
                        '\n\n\n'
                    ).format(
                        coords[2],
                        coords[1],
                        coords[0]
                    )
                    head4 = (
                        'TEMPERATURE (°C), RAIN (mm), PRESSURE (hPa),' +
                        ' HUMIDITY (%), WIND SPEED (m/s)\n\n'
                    )

                    head5 = '{}Avg\n'.format(self.create_spaces(62))
                    head6 = '{}Mean{}Mean{}Mean{}Wind{}DOM\n'.format(
                        self.create_spaces(5),
                        self.create_spaces(39),
                        self.create_spaces(4),
                        self.create_spaces(2),
                        self.create_spaces(16),
                    )
                    head7 = (
                        'Day{}Temp{}Max{}Time{}Min{}Time{}' +
                        'Rain{}Press{}Hum{}SPEED{}Max{}Time{}Dir{}\n'
                    ).format(
                        self.create_spaces(2),
                        self.create_spaces(4),
                        self.create_spaces(3),
                        self.create_spaces(4),
                        self.create_spaces(3),
                        self.create_spaces(4),
                        self.create_spaces(3),
                        self.create_spaces(3),
                        self.create_spaces(3),
                        self.create_spaces(2),
                        self.create_spaces(2),
                        self.create_spaces(4),
                        self.create_spaces(4)
                    )
                    output_txt += head1
                    output_txt += head2
                    output_txt += head3
                    output_txt += head4
                    output_txt += head5
                    output_txt += head6
                    output_txt += head7
                    output_txt += self.create_spaces(87, char='-')
                    output_txt += '\n'

                    for d in data:
                        output_txt += self.generate_str_row(d)
                    output_txt += self.create_spaces(87, char='-')
                    output_txt += '\n'

                    last_row = [
                        '', t_avg, t_highest,
                        t_max_idx, t_lowest, t_min_idx,
                        r_sum, p_avg, h_avg,
                        ws_avg, ws_highest, ws_max_idx, wd_avg
                    ]
                    last_row = list(
                        map(
                            lambda x: '' if str(x) == 'nan' else x,
                            last_row
                        )
                    )
                    output_txt += self.generate_str_row(last_row)

                    output_txt += '\nMax >={}{}\n'.format(
                        self.create_spaces(1, 5, self.thresholds['tmax_eh']),
                        self.create_spaces(1, 2, tmax_eh)
                    )
                    output_txt += 'Max <={}{}\n'.format(
                        self.create_spaces(1, 5, self.thresholds['tmax_el']),
                        self.create_spaces(1, 2, tmax_el)
                    )
                    output_txt += 'Min <={}{}\n'.format(
                        self.create_spaces(1, 5, self.thresholds['tmin1_el']),
                        self.create_spaces(1, 2, tmin1_el)
                    )
                    output_txt += 'Min <={}{}\n'.format(
                        self.create_spaces(1, 5, self.thresholds['tmin2_el']),
                        self.create_spaces(1, 2, tmin2_el)
                    )
                    output_txt += 'Max Rain: {} on day {}\n'.format(
                        r_max,
                        r_max_idx
                    )
                    output_txt += (
                        'Days od Rain:{} (>={} mm){} (>={} mm){} (>={} mm)\n'
                    ).format(
                        self.create_spaces(1, 2, r1_eh),
                        self.create_spaces(1, 2, self.thresholds['r1_eh']),
                        self.create_spaces(2, 2, r2_eh),
                        self.create_spaces(1, 2, self.thresholds['r2_eh']),
                        self.create_spaces(2, 2, r3_eh),
                        self.create_spaces(1, 2, self.thresholds['r3_eh']),
                    )
                    file_name = "{}_{}_{}.txt".format(
                        stat_name,
                        self.month_name,
                        self.year
                    )
                    with open(
                        os.path.join(
                            self.base_path,
                            'data',
                            'monthly',
                            file_name), "w") as f:
                        f.write(output_txt)

    def execute(self):
        for station in self.stations:
            if not station['df_checked'].empty:
                # Procedure name
                procedure = station['name'].split(':')[-1]

                # DESTINATION istSOS CONFIG ============================
                # Location (if not given, same as source will be used)
                durl = self.base_url
                # Service instance name
                dsrv = self.dservice

                # PROCESSING STARTS HERE ================================
                self.logger.info("istSOS > 2 > istSOS STARTED:")
                self.logger.info("==============================\n")

                # Load procedure description
                self.logger.info("1. Loading procedure description: %s" % procedure)

                # Loading describe sensor from source ===================
                try:
                    res = requests.get(
                        "%s/wa/istsos/services/%s/procedures/%s" % (
                            self.base_url,
                            self.service,
                            procedure
                        ),
                        auth=self.basic_auth,
                        verify=True
                    )

                    sdata = res.json()
                    res.close()
                    if sdata['success'] is False:
                        msg = (
                            "Description of procedure %s"
                            " can not be loaded from "
                            "source service: %s" % (
                                procedure, sdata['message']
                            )
                        )
                        self.logger.error(msg)
                        raise Exception(msg)
                    else:
                        self.logger.info("   > DS Source Ok.")
                except Exception as e:
                    res.close()
                    msg = (
                        "(%s) Error during %s DS loading from "
                        "source service %s:" % (
                            self.event_time,
                            procedure, str(e)
                        )
                    )
                    self.logger.error(msg)

                # Loading and sync describe sensor from destination
                while(True):
                    try:
                        res = requests.get(
                            "%s/wa/istsos/services/%s/procedures/%s" % (
                                durl, dsrv, procedure
                            ),
                            auth=self.basic_auth,
                            verify=True
                        )
                        ddata = res.json()
                        res.close()
                        if ddata['success'] is False:
                            self.logger.info((
                                "   > Creating procedure into "
                                "destination service..."
                            ))
                            outputs = sdata['data']['outputs']
                            for output in outputs:
                                if 'time' not in output['definition']:
                                    uom = output['uom']
                                    observedproperty = {
                                        'definition': output['definition'],
                                        'name': output['name']
                                    }
                                    if 'description' in output:
                                        observedproperty[
                                            'description'
                                        ] = output['description']
                                    if 'constraint' in output:
                                        observedproperty[
                                            'constraint'
                                        ] = output['constraint']
                                    else:
                                        observedproperty['constraint'] = {}

                                    res = requests.post(
                                        "%s/wa/istsos/services/%s/uoms" % (
                                            durl, dsrv
                                        ),
                                        auth=self.basic_auth,
                                        data=json.dumps({
                                            'description': '',
                                            'name': uom
                                        })
                                    )

                                    res_out = res.json()
                                    res.close()
                                    if (res_out['success'] or
                                        'already exists' in res_out[
                                            'message']):
                                        res = requests.post(
                                            (
                                                "%s/wa/istsos/services"
                                                "/%s/observedproperties"
                                            ) % (
                                                durl, dsrv
                                            ),
                                            auth=self.basic_auth,
                                            data=json.dumps(
                                                observedproperty
                                            )
                                        )
                                        res_out = res.json()
                                        res.close()
                                        if not res_out['success']:
                                            if 'already exists' not in res_out[
                                                    'message']:
                                                msg = (
                                                    "Observed property %s can "
                                                    "not be registered from "
                                                    "destination service: "
                                                    "%s" % (
                                                        observedproperty[
                                                            'name'
                                                        ],
                                                        res_out['message']
                                                    )
                                                )
                                                self.logger.error(msg)
                                                raise Exception(msg)
                                            else:
                                                res_out['success'] = True

                                    else:
                                        msg = (
                                            "(%s) UOM %s ca not be registered"
                                            " from "
                                            "destination service: %s" % (
                                                self.event_time,
                                                uom,
                                                res_out['message']
                                            )
                                        )
                                        self.logger.error(msg)
                                        raise Exception(msg)

                            if (res_out['success'] or
                                'already exists' in res_out[
                                    'message']):

                                res = requests.post(
                                    "%s/wa/istsos/services/%s/procedures" % (
                                        durl, dsrv
                                    ),
                                    auth=self.basic_auth,
                                    verify=False,
                                    data=json.dumps(
                                        sdata['data']
                                    )
                                )
                                res_out = res.json()
                                res.close()
                                if not res_out['success']:
                                    msg = (
                                        "(%s) Procedure %s can not"
                                        " be registered from "
                                        "destination service: %s" % (
                                            self.event_time,
                                            procedure,
                                            res_out['message']
                                        )
                                    )
                                    self.logger.error(msg)
                                    raise Exception(msg)
                        else:
                            self.logger.info("   > DS Destination Ok.")
                            break
                    except Exception as e:
                        res.close()
                        msg = (
                            "(%s) Error during %s DS syncing: %s" % (
                                self.event_time,
                                procedure,
                                str(e)
                            )
                        )
                        self.logger.error(msg)

                try:
                    # Load of a getobservation template from destination
                    res = requests.get(
                        (
                            "%s/wa/istsos/services/%s/operations/"
                            "getobservation/offerings/%s/procedures/"
                            "%s/observedproperties/:/eventtime/"
                            "last?qualityIndex=False"
                        ) % (
                            durl, dsrv, 'temporary', procedure
                        ),
                        params={
                            "qualityIndex": False
                        },
                        auth=self.basic_auth
                    )
                    dtemplate = res.json()
                    res.close()
                    if dtemplate['success'] is False:
                        msg = (
                            "Observation template of procedure %s can not be "
                            "loaded: %s" % (procedure, dtemplate['message'])
                        )
                        self.logger.error(msg)
                        raise Exception(msg)
                    else:
                        dtemplate = dtemplate['data'][0]
                        dtemplate[
                            'AssignedSensorId'
                        ] = ddata['data']['assignedSensorId']
                        dtemplate['result']['DataArray']['values'] = []
                        self.logger.info("     > GO Template Ok.")
            # #     for output in sdata['data']['outputs']:
            # #         if 'time' not in output['definition']:

                    self.logger.info("2. Identifying processing interval:")

                    # Check if mesaures are present in source procedure,
                    # by identifying the
                    # sampling time constraint located always
                    # in the first position of the
                    # outputs, if it is empty an exception is thrown
                    if ('constraint' not in sdata['data']['outputs'][0] or
                        'interval' not in sdata['data'][
                            'outputs'][0]['constraint']):
                        msg = (
                            "There is no data in the source procedure "
                            "to be copied to the "
                            "destination procedure."
                        )
                        self.logger.error(msg)
                        raise Exception(msg)
                    else:
                        # Check if the contraint interval contains
                        # a valid ISO date begin position
                        try:
                            iso.parse_datetime(
                                sdata['data'][
                                    'outputs'
                                ][0]['constraint']['interval'][0]
                            )
                        except Exception:
                            msg = (
                                "The date in the source procedure constraint "
                                "interval (%s) is not valid." %
                                sdata['data']['outputs'][0][
                                    'constraint']['interval'][0]
                            )
                            self.logger.error(msg)
                            raise Exception(msg)

                        # Check if the contraint interval contains
                        # a valid ISO date
                        # end position
                        try:
                            iso.parse_datetime(
                                sdata['data']['outputs'][0][
                                    'constraint']['interval'][1])
                        except Exception:
                            msg = (
                                "The date in the source procedure constraint "
                                "interval (%s) is not valid." %
                                sdata['data']['outputs'][0][
                                    'constraint']['interval'][1]
                            )
                            self.logger.error(msg)
                            raise Exception(msg)

                    self.logger.info("   > Source interval is valid")

                    outputs = sdata['data']['outputs']

                    begin_end = self.event_time.split('/')
                    start = iso.parse_datetime(begin_end[0])+timedelta(days=1)

                    start = start.replace(hour=0)
                    self.report_date = start.strftime("%Y-%m-%d")

                    stop = iso.parse_datetime(begin_end[1])
                    stop = stop.replace(hour=0)

                    self.logger.info("   > Destination interval is valid")
                    self.logger.info("   > Start processing: %s" % start)
                    self.logger.info("   > Stop processing: %s" % stop)
                    dtemplate["samplingTime"] = {}

                    dtemplate["samplingTime"][
                        "beginPosition"] = start.isoformat()
                    dtemplate["samplingTime"][
                        "endPosition"] = stop.isoformat()
                    cols = []
                    definitions = []
                    for output in outputs:
                        definitions.append(output)
                        cols.append(output['definition'])
                        if 'time' not in output['definition']:
                            definitions.append({
                                'definition': (
                                    output['definition'] +
                                    ':qualityIndex'
                                ),
                                'name': output['name']+':qualityIndex',
                                'code': '-'
                            })
                            cols.append(output['definition']+':qualityIndex')
                    col_idx_name = station['df_checked'].index.name
                    station[
                        'df_checked'
                    ][col_idx_name] = station['df_checked'].index
                    station[
                        'df_checked'
                    ][col_idx_name] = station['df_checked'][
                        col_idx_name
                    ].apply(lambda x: x.isoformat())
                    df_data = station['df_checked'][cols]

                    df_data_list = df_data[
                        start.isoformat():stop.isoformat()
                    ].to_numpy().tolist()

                    dtemplate['result']['DataArray']['values'] = list(
                        map(
                            lambda x: list(
                                map(
                                    lambda y: self.format_value(y), x
                                )
                            ),
                            df_data_list
                        )
                    )

                    dtemplate['result']['DataArray']['field'] = definitions
                    dtemplate['observedProperty']['component'] = cols
                    dtemplate['observedProperty'][
                        'CompositePhenomenon'
                    ]['dimension'] = str(len(cols))

                    # POST data to WA
                    res = requests.post((
                            "%s/wa/istsos/services/"
                            "%s/operations/insertobservation"
                        ) % (
                        durl,
                        dsrv),
                        auth=self.basic_auth,
                        verify=True,
                        data=json.dumps({
                            "ForceInsert": 'true',
                            "AssignedSensorId": ddata['data'][
                                'assignedSensorId'],
                            "Observation": dtemplate
                        })
                    )

                    res_json = res.json()
                    res.close()

                    # read response
                    self.logger.info(
                        (
                            "     > Insert observation "
                            "success: %s"
                        ) % res_json['success'])

                    if not res_json['success']:
                        msg = 'Error inserting observation: %s' % (
                            res_json['message']
                        )
                        self.logger.error(msg)
                        raise Exception(msg)
                except Exception as e:
                    res.close()
                    msg = (
                        "(%s) Error during %s GO syncing: %s" % (
                            self.event_time,
                            procedure,
                            str(e)
                        )
                    )
                    self.logger.error(msg)

    def format_value(self, y):
        if y == 'nan':
            return '-999.99'
        else:
            try:
                value_ = str(y.round(2))
            except:
                value_ = str(y)

            return value_

    def create_daily_report(self):
        directory = os.path.join(self.base_path, 'notification')
        if not os.path.exists(directory):
            os.makedirs(directory)
        report_name = '4onse_{}.md'.format(
            self.report_date
        )
        self.report_name = report_name
        f = open(
            os.path.join(
                self.base_path,
                'notification',
                report_name
            ),
            "w"
        )
        f.write(
            f'*_{self.day}/{self.month}/{self.year} 4ONSE DAILY REPORT_*\n'
        )
        f.write('!!!\n')
        f.write('!!!\n')

        for stat in self.stations:
            stat_name = '{}\n'.format(stat['name'].split(':')[-1])
            f.write(
                '*{}*'.format(stat_name)
            )
            station_stuck = False
            try:
                stat['df_checked'][self.report_date]
            except:
                station_stuck = True
            if stat['df_checked'].empty or station_stuck:
                f.write('Station is *not* working\n')
            else:
                data_perc = (
                    stat['df_checked'][self.report_date].count()/144*100
                ).round(1)[0]
                f.write('Data completeness: {} % \n'.format(data_perc))
                for col in stat['df_checked'].columns:
                    if ('time' not in col and
                        'quality' not in col and
                        'procedure' not in col and
                        'solar' not in col and
                            'soil' not in col):
                        if col in self.plausible_value:
                            methods = self.plausible_value[col]['methods']
                            check = False
                            if 'm' in methods:
                                qi_df = stat['df_checked']
                                count = stat['df_checked'][self.report_date][
                                    col+':qualityIndex'] == 710
                                if count.sum() < data_perc:
                                    check = True
                            elif 't' in methods:
                                qi_df = stat['df_checked']
                                count = stat['df_checked'][self.report_date][
                                    col+':qualityIndex'] == 705
                                if count.sum() < data_perc:
                                    check = True
                            elif 'p' in methods:
                                qi_df = stat['df_checked']
                                count = stat['df_checked'][self.report_date][
                                    col+':qualityIndex'] == 700
                                if count.sum() < data_perc:
                                    check = True
                            else:
                                check = True

                            if check:
                                f.write(
                                    '\nCheck *{}*\n'.format(
                                        col[35:]
                                    )
                                )
            f.write('!!!\n')
        f.close()

    def zenodo_publish_day(self):
        try:
            os.chdir(
                os.path.join(
                    self.base_path, 'data', 'daily'
                )
            )
            list_files = os.listdir()

            zip_name = '4onse_{}_daily_data.zip'.format(
                self.month_name.lower()
            )

            with ZipFile(os.path.join(
                        self.base_path, 'data', 'daily', zip_name
                    ), 'w') as myzip:
                for i in list_files:
                    if '.txt' in i:
                        month = i.split('_')[-1][2:4]
                        if self.month == int(month):
                            myzip.write(i)

            os.chdir(self.base_path)

            self.logger.info("> Start uploading to Zenodo")

            r = requests.get(
                "{}/api/deposit/depositions".format(
                    self.zenodo_url
                ),
                params={
                    "q": self.deposition_daily['metadata']['title'],
                    "access_token": self.zenodo_token
                }
            )
            r.status_code
            res = r.json()
            r.close()
            if res:
                self.logger.info("    > Deposition OK")
                deposition_id = res[0]['id']
                headers = {"Content-Type": "application/json"}
                r = requests.put(
                    "{}/api/deposit/depositions/{}".format(
                        self.zenodo_url,
                        deposition_id
                    ),
                    params={
                        'access_token': self.zenodo_token
                    },
                    headers=headers,
                    json=self.deposition_daily
                )
                if r.status_code <= 201:
                    self.logger.info("    > Deposition Updated")
                    res = r.json()
                    r.close()
                    if 'files' in res:
                        if res['files']:
                            for file_ in res['files']:
                                if file_['filename'] == zip_name:
                                    files_id = file_['id']

                                    url = (
                                        '{}/api/deposit/depositions/'
                                        '{}/files/{}'
                                    ).format(
                                        self.zenodo_url,
                                        deposition_id, files_id
                                    )
                                    r = requests.delete(
                                        url,
                                        params={
                                            'access_token': self.zenodo_token
                                        }
                                    )
                                    if r.status_code != 204:
                                        self.logger.info(
                                            (
                                                "    > "
                                                "Error in deleting File"
                                            )
                                        )
                                        r.close()
                                    r.close()
                                    self.logger.info("    > File deleted")
                    self.logger.info("    > Creating File")
                    url = '{}/api/deposit/depositions/{}/files'.format(
                        self.zenodo_url,
                        deposition_id
                    )
                    data = {
                        'filename': zip_name
                    }
                    files = {
                        'file': open(
                            os.path.join(
                                self.base_path,
                                'data',
                                'daily',
                                zip_name
                            ),
                            'rb'
                        )
                    }
                    r = requests.post(
                        url,
                        params={
                            'access_token': self.zenodo_token
                        },
                        data=data,
                        files=files
                    )
                    r.close()
                    if r.status_code == 201:
                        self.logger.info("    > File OK")
                    else:
                        self.logger.info("    > Error in creating File")

                else:
                    self.logger.info("    > Error during deposition updating")

            else:
                self.logger.info("    > Creating deposition")
                headers = {"Content-Type": "application/json"}
                r = requests.post(
                    "{}/api/deposit/depositions".format(
                        self.zenodo_url
                    ),
                    params={
                        'access_token': self.zenodo_token
                    },
                    headers=headers,
                    json=self.deposition_daily
                )
                r.status_code
                r.close()
                if r.status_code <= 201:
                    self.logger.info("    > Deposition OK")
                    res = r.json()
                    r.close()
                    deposition_id = res['id']
                    data = {'filename': zip_name}
                    files = {
                        'file': open(
                            os.path.join(
                                self.base_path,
                                'data',
                                'daily',
                                zip_name
                            ),
                            'rb'
                        )
                    }
                    r = requests.post(
                        '{}/api/deposit/depositions/{}/files'.format(
                            self.zenodo_url,
                            deposition_id
                        ),
                        params={'access_token': self.zenodo_token},
                        data=data,
                        files=files
                    )
                    if r.status_code <= 201:
                        res = r.json()
                        r.close()
                        self.logger.info("    > File OK")
                    else:
                        self.logger.info("    > Error in creating the File")
                else:
                    self.logger.info("    > Error in creating the Deposition")
        except Exception as e:
            os.chdir(self.base_path)
            raise e

    def zenodo_publish_monthly(self):
        if self.today.month != self.month:
            try:
                os.chdir(
                    os.path.join(
                        self.base_path, 'data', 'monthly'
                    )
                )
                list_files = os.listdir()
                self.logger.info("> Start uploading to Zenodo")

                r = requests.get(
                    "{}/api/deposit/depositions".format(
                        self.zenodo_url
                    ),
                    params={
                        "q": self.deposition_monthly['metadata']['title'],
                        "access_token": self.zenodo_token
                    }
                )
                r.status_code
                res = r.json()
                r.close()
                create_action = True
                for deposition in res:
                    if self.deposition_monthly[
                            'metadata']['title'] == deposition['title']:
                        create_action = False
                        deposition_id = deposition['id']
                        break
                headers = {
                    "Content-Type": "application/json"
                }
                if create_action:
                    self.logger.info("    > Creating deposition")

                    r = requests.post(
                        "{}/api/deposit/depositions".format(
                            self.zenodo_url
                        ),
                        params={
                            'access_token': self.zenodo_token
                        },
                        headers=headers,
                        json=self.deposition_monthly
                    )
                    res = r.json()
                    deposition_id = res['id']
                    r.close()
                    if r.status_code <= 201:
                        self.logger.info("    > Deposition OK")
                        for report in list_files:
                            data = {'filename': report}
                            report_data = {
                                'file': open(
                                    os.path.join(
                                        self.base_path,
                                        'data',
                                        'monthly',
                                        report
                                    ),
                                    'rb'
                                )
                            }
                            r = requests.post(
                                '{}/api/deposit/depositions/{}/files'.format(
                                    self.zenodo_url,
                                    deposition_id
                                ),
                                params={'access_token': self.zenodo_token},
                                data=data,
                                files=report_data
                            )
                            if r.status_code <= 201:
                                res = r.json()
                                r.close()
                                self.logger.info("    > File OK")
                            else:
                                self.logger.info("    > Error in creating the File")
                else:
                    r = requests.put(
                        "{}/api/deposit/depositions/{}".format(
                            self.zenodo_url,
                            deposition_id
                        ),
                        params={
                            'access_token': self.zenodo_token
                        },
                        headers=headers,
                        json=self.deposition_monthly
                    )
                    if r.status_code <= 201:
                        self.logger.info("    > Deposition Updated")
                        res = r.json()
                        r.close()
                        if 'files' in res:
                            if res['files']:
                                for file_ in list_files:
                                    upload = True
                                    for file2_ in res['files']:
                                        if file_ == file2_:
                                            upload = False

                                    if upload:
                                        self.logger.info("    > Creating File")
                                        url = '{}/api/deposit/depositions/{}/files'.format(
                                            self.zenodo_url,
                                            deposition_id
                                        )
                                        data = {'filename': file_}
                                        report_data = {
                                            'file': open(
                                                os.path.join(
                                                    self.base_path,
                                                    'data',
                                                    'monthly',
                                                    file_
                                                ),
                                                'rb'
                                            )
                                        }
                                        r = requests.post(
                                            url,
                                            params={
                                                'access_token': self.zenodo_token
                                            },
                                            data=data,
                                            files=report_data
                                        )
                                        r.close()
                                        if r.status_code == 201:
                                            self.logger.info(f"    > {file_} UPLOADED")
                                        else:
                                            self.logger.info(f"    > Error in creating {file_}")
                    else:
                        self.logger.info("    > Error during deposition updating")
            except Exception as e:
                self.logger.error("Errors")
                self.logger.error(str(e))

    def telegram_bot_sendtext(self, bot_message, chat_id):
        bot_token = self.telegram_token
        bot_chatID = str(chat_id)
        send_text = (
            'https://api.telegram.org/bot' +
            bot_token +
            '/sendMessage?chat_id=' +
            bot_chatID +
            '&parse_mode=Markdown&text=' +
            bot_message
        )

        response = requests.get(send_text)
        response_json = response.json()
        response.close()

        return response_json

    def telegram_msg(self):
        db = TinyDB(os.path.join(
            self.base_path, self.telegram_db
        ))
        Users = Query()
        users = db.search(Users.sub == True)
        f = open(os.path.join(
            self.base_path,
            'notification',
            self.report_name
        ), "r")
        text = f.read()
        lista = text.split('!!!\n')
        f.close()
        send_text = ''
        send_text_list = []
        for i in lista:
            if len(send_text) >= 3000:
                send_text_list.append(send_text)
                send_text = ''
            else:
                send_text += '\n{}'.format(i)
        if len(send_text) != 0:
            send_text_list.append(send_text)
        if len(send_text_list) != 0:
            for user in users:
                for send_text in send_text_list:
                    self.telegram_bot_sendtext(send_text, user['chat_id'])

    def main(self):
        self.logger.info(self.base_url)
        self.logger.info(self.event_time)
