#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# Copyright (C) 2024 makischu

# proof of concept for
# - scheduling the following:
#   - reading MC303 energy meter via mbus using a ethernet serial converter
#   - decode its telegram using wmbusmeters (not python)
#   - select elements of interest and publish to mqtt 

# although wmbusmeters also offers scheduling and mqtt options, 
# I chose to use a python script instead, for easier adaptions.


# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import paho.mqtt.client as mqtt
import time
import datetime
import signal 
import socket
import json
import logging
import subprocess


mbus_host="192.168.x.x"     #adopt to yours
mbus_port=8234              #adopt to yours
mbus_addr=b'\x30'           #adopt to yours
mbus_snum='30303030'        #adopt to yours

mqtt_host = "192.168.x.x"   #adopt to yours
mqtt_port = 1883
mqtt_topic= "dev/mbu/mc3/telemetry"

path_to_wmbusmeters = '/path/to/wmbusmeters' #adopt to yours


clientStrom = mqtt.Client()
logging.info('Starting mqtt...')
clientStrom.connect(mqtt_host, mqtt_port, 60)
clientStrom.loop_start()

run = True
def handler_stop_signals(signum, frame):
    global run
    run = False
signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)



def publish2mqtt(values):
    global mqtt_topic
    messageJson = json.dumps(values)
    clientStrom.publish(mqtt_topic, messageJson)
    #{"src": "mbus2mqtt", "error": "0", "t": "2024-10-18T21:43:00", "t_collect[ms]": "798", "P[kW]": 0, "E_heat[kWh]": 195, "E_cool[kWh]": 0, "E_forw[m3C]": 1852, "E_back[m3C]": 1683, "T_forw[C]": 32.04, "T_back[C]": 28.09, "T_diff[C]": 3.95, "Vol_flow[m3h]": 0, "status": "OK"}
    return


def decode_mbus_telegram(data):
    global mbus_snum
    global path_to_wmbusmeters
    error = 0
    values = {}
    hexdata = ''
    selection = {'power_kw' : 'P[kW]',
                 'total_energy_consumption_kwh' : 'E_heat[kWh]',
                 'total_energy_backward_kwh' : 'E_cool[kWh]',
                 'forward_energy_m3c' : 'E_forw[m3C]', 
                 'return_energy_m3c' : 'E_back[m3C]', 
                 't1_temperature_c' : 'T_forw[C]',
                 't2_temperature_c' : 'T_back[C]',
                 'flow_return_temperature_difference_c' : 'T_diff[C]',
                 'volume_flow_m3h' : 'Vol_flow[m3h]',
                 'status': 'status', 
                 'total_volume_m3' : 'Vol[m3]'}
    try:
        hexdata = data.hex()
        #use the great wmbusmeters project for the actual decoding. 
        command = '{wmbusmeters} --format=json {data}  MyMC303 kamheat {snum} NOKEY'.format(data=hexdata, snum=mbus_snum, wmbusmeters=path_to_wmbusmeters)
        stat, resp = subprocess.getstatusoutput(command)
        #{"media":"heat/cooling load","meter":"kamheat","name":"MyMC303","id":"30303030","flow_return_temperature_difference_c":0.35,"forward_energy_m3c":1829,"max_flow_m3h":1.222,"max_power_kw":18,"on_time_h":2729,"on_time_at_error_h":0,"power_kw":0,"return_energy_m3c":1663,"t1_temperature_c":20.44,"t2_temperature_c":20.09,"target_energy_kwh":0,"target_volume_m3":0,"total_energy_backward_kwh":0,"total_energy_consumption_kwh":190,"total_volume_m3":52.95,"volume_flow_m3h":0,"status":"OK","target_date":"2000-00-00","timestamp":"2024-10-18T18:07:50Z"}
        respdata = json.loads(resp)
        assert respdata['name']=='MyMC303'
        for key in selection:
            values[selection[key]] = respdata[key]  if  key in respdata  else '-'
    except:
        logging.error('decode_mbus_telegram fail. ' + hexdata)
        error = 1
    return error, values


def collect_mbus():
    global mbus_addr
    error = 0
    values = {}
    try:
        mbussocket = socket.create_connection((mbus_host, mbus_port), timeout=5)
        #send request
        request = b'\x10\x5B' + mbus_addr + bytes([b'\x5B'[0]+ mbus_addr[0]]) + b'\x16'
        mbussocket.send(request)
        #receive response
        data = mbussocket.recv(512)
        mbussocket.close()
        if not data:
            logging.error('collect_mbus fail. no response from mbus bridge')
            error = 1
        else:
            #parse
            error, values = decode_mbus_telegram(data)
    except Exception as e:
        logging.error('collect_mbus fail. ' + str(e))
        error = 1
    return error, values


def work_every_minute():
    t = datetime.datetime.now().isoformat(timespec='seconds')
    tstart = time.time()
    #receive data:
    error, values = collect_mbus()
    tcollect_ms = int((time.time() -tstart) * 1000.0)
    #publish data:
    heads = { "src" : "mbus2mqtt", 
              "error" : str(error), 
              "t" : t ,
              "t_collect[ms]" : str(tcollect_ms) }
    row = {}
    row.update(heads)
    row.update(values)
    publish2mqtt(row)
    return


while run:
    mynow   = datetime.datetime.now()
    #work every minute
    if mynow.second % 60 == 0:        
        work_every_minute()

    #wait for next full second
    time.sleep(1.0 - (time.time() % 1.0))


logging.info('Stopping mqtt...\n')
clientStrom.loop_stop()

