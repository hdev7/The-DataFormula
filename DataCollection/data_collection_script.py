"""
@Project: Real time stream processing of formula 1 racing car telemetry
@author: hemanth
"""


import socket
import structs
import rawutil
from pprint import pprint


UDP_IP = "127.0.0.1" # UDP listen IP-address
UDP_PORT = 20777 # UDP listen port
PACKET_SIZE = 1289 # Amount of bytes in packet



# create a socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# bind the socket to the specified ip address and port
sock.bind((UDP_IP, UDP_PORT))
print ("F1 Telemetry ready")
print ("Listening on " + UDP_IP + ":" + str(UDP_PORT))




print("ses_t,lap_t,lap_d,ses_d,speed,steer,gear,rpm,fuel,fuel_cap,brake_temp,tyre_p_rl,tyre_p_rr,tyre_p_fl,tyre_p_fr,tyre_t_rl,tyre_t_rr,tyre_t_fl,tyre_t_fr")
while True:
    data, address = sock.recvfrom(PACKET_SIZE)
    ses_t, = rawutil.unpack('<f',data[0:4])
    lap_t, = rawutil.unpack('<f',data[4:8])
    lap_d, = rawutil.unpack('<f',data[8:12])
    ses_d, = rawutil.unpack('<f',data[0:4])
    speed, = rawutil.unpack('<f',data[28:32])
    steer, = rawutil.unpack('<f',data[120:124])
    gear, = rawutil.unpack('<f',data[132:136])
    rpm, = rawutil.unpack('<f',data[148:152])
    fuel, = rawutil.unpack('<f',data[180:184])
    fuel_cap, = rawutil.unpack('<f',data[184:188])
    brake_temp, = rawutil.unpack('<f',data[204:208])
    tyre_p_rl, = rawutil.unpack('<f',data[220:224])
    tyre_p_rr, = rawutil.unpack('<f',data[224:228])
    tyre_p_fl, = rawutil.unpack('<f',data[228:232])
    tyre_p_fr, = rawutil.unpack('<f',data[232:236])
    tyre_t_rl, = rawutil.unpack('<b',data[304:305])
    tyre_t_rr, = rawutil.unpack('<b',data[305:306])
    tyre_t_fl, = rawutil.unpack('<b',data[306:307])
    tyre_t_fr, = rawutil.unpack('<b',data[307:308])
    

    print("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(round(ses_t,2),round(lap_t,2),round(lap_d,2),round(ses_d,2),round(speed,2),round(steer,2),gear,round(rpm,2),round(fuel,2),round(fuel_cap,2),round(brake_temp,2),round(tyre_p_rl,2),round(tyre_p_rr,2),round(tyre_p_fl,2),round(tyre_p_fr,2),tyre_t_rl,tyre_t_rr,tyre_t_fl,tyre_t_fr))





