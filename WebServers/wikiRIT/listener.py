'''
__Author__ : Shashank Gangadhara
'''

import subprocess
import socket
import json

#
# count = 0
#
#
# def receive():
#
#     global count
#
#     IP = "129.21.156.120"
#     port = 6007
#
#     receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#     receivingSock.bind((IP, port))
#
#     print("Started Receiving from web server")
#
#     while True:
#
#         data, addr = receivingSock.recvfrom(1024)
#
#         print("Received something")
#         tempCheck = data.decode('utf-8')
#
#         if tempCheck == "Create" :
#
#             container = subprocess.call('startContainer.sh')
#
#             if container.wait()!=0:
#                 print("Could not build container")
#
#             count+=1


def send():

    data = {}

    data['flag'] = "Insert"
    data['article']
    data = json.dumps(data)
    data = data.encode("utf-8")
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))


def main():


    send()

if __name__ == '__main__':
    main(0)