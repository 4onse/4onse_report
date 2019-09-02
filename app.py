# -*- coding: utf-8 -*-
# ===============================================================================
#
# Authors: Daniele Strigaro
#
# Copyright (c) 2019 IST-SUPSI (www.supsi.ch/ist)
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# ===============================================================================

from gen_report import GenReport
import json
import os
from datetime import datetime, timedelta
import pytz
import logging

base_path = os.path.dirname(__file__)

# SET LOGGER
# Enable logging
LOG_INFO = os.path.join(
    base_path,
    'report.log'
)

LOG_ERROR = os.path.join(
    base_path,
    'report_error.log'
)

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logger = logging.getLogger(__name__)
log_formatter = logging.Formatter(LOG_FORMAT)

# comment this to suppress console output
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

file_handler_info = logging.FileHandler(LOG_INFO, mode='a')
file_handler_info.setFormatter(log_formatter)
file_handler_info.setLevel(logging.INFO)
logger.addHandler(file_handler_info)

file_handler_error = logging.FileHandler(LOG_ERROR, mode='a')
file_handler_error.setFormatter(log_formatter)
file_handler_error.setLevel(logging.ERROR)
logger.addHandler(file_handler_error)

logger.setLevel(logging.INFO)

with open(
    os.path.join(
        base_path,
        'config.json'
        ), 'r') as f:
    conf = json.load(f)


def check_missing_days(conf, base_path):
    first_day_str = conf['begin']
    first_day = datetime.strptime(
        first_day_str,
        "%d%m%Y"
    ).date()
    tz = pytz.timezone(conf['tz'])
    today = datetime.now(tz)
    today_str = today.strftime("%d%m%Y")
    expected_days = []
    tmp_day = first_day
    while tmp_day <= today.date():
        expected_days.append(tmp_day.strftime("%d%m%Y"))
        tmp_day = tmp_day + timedelta(days=1)

    daily_file_list = os.listdir(
        os.path.join(
            base_path,
            'data',
            'daily'
        )
    )
    missing_days = []

    for i in expected_days:
        add = True
        for d in daily_file_list:
            date_str = d.split('_')[-1].strip('.txt')
            if date_str == i:
                add = False

        if add:
            missing_days.append(i)

    return missing_days


def check_missing_months(conf, base_path):
    first_day_str = conf['begin']
    first_month = datetime.strptime(
        first_day_str[2:],
        "%m%Y"
    ).date()
    tz = pytz.timezone(conf['tz'])
    today = datetime.now(tz)
    today_str = today.strftime("%m%Y")
    expected_months = []
    tmp_month = first_month
    while tmp_month <= today.date():
        expected_months.append(tmp_month.strftime("%B_%Y"))
        tmp_month = tmp_month.replace(month=tmp_month.month+1)

    monthly_file_list = os.listdir(
        os.path.join(
            base_path,
            'data',
            'monthly'
        )
    )
    missing_months = []

    for i in expected_months:
        add = True
        for m in monthly_file_list:
            date_str = '_'.join(
                m.strip('.txt').split('_')[-2:]
            )
            if date_str == i:
                add = False

        if add:
            missing_months.append(i)

    return missing_months


missing_days = check_missing_days(conf, base_path)
missing_months = check_missing_months(conf, base_path)


############################
# START DAILY STATS REPORT #
############################

for d in missing_days:
    conf['year'] = int(d[-4:])
    conf['month'] = int(d[2:4])
    conf['day'] = int(d[0:2])

    rep = GenReport(
        conf,
        logger=logger
    )
    rep.execute()
    rep.create_daily_report()
    rep.telegram_msg()

##############################
# START MONTHLY STATS REPORT #
##############################

for m in missing_months:

    conf['type'] = 'monthly'
    month = datetime.strptime(
        m,
        "%B_%Y"
    ).month
    conf['year'] = int(m[-4:])
    conf['month'] = month
    rep = GenReport(
        conf,
        logger=logger
    )
    rep.export_report()
    rep.zenodo_publish_monthly()
