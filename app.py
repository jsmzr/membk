import re
import socket
import os
import sys
from multiprocessing import Queue, Process
import time
import pickle

time_out = 3

mem_host = '127.0.0.1'
mem_port = 11211
operat_type = ''
backup_host = '192.168.32.128'
backup_port = 11211
file_path = '.'
file_name = ''



# mem command
CMD_STATS = b'stats \r\n'
CMD_STATS_ITEMS = b'stats items \r\n'

CMD_ERROR = b'ERROR\r\n'
CMD_CLIENT_ERROR = b'CLIENT_ERROR bad data chunk\r\n'
CMD_STORED = b'STORED\r\n'

DATA_HEAD = b'VALUE'
DATA_HEAD_LEN = len(DATA_HEAD)
DATA_FOOT = b'END\r\n'
DATA_FOOT_LEN = len(DATA_FOOT)
DATA_SUCCESS_FOOT_LEN = len(b'\r\nEND\r\n')
DATA_BREAK_LEN = len(b'\r\n')

'''
set <key> <flags> <time> <bytes> /r/n <value> /r/n
get <key>
stats cachedump <items> <number> 
'''
cmd_set = 'set {0} {1} {2} {3}\r\n'
cmd_set_foot = b'\r\n'
cmd_get = 'get {0} \r\n'
cmd_cachedump = 'stats cachedump {0} {1} \r\n'

'''
stats_info:
STAT <attribute> <value>

items_info:
STAT items:<id>:<attribute> <value>

key_info:
ITEM <key> [<bytrs>; <exptime>]

itme:
VALUE <key> <flags> <bytes>
<data> 
'''

def usage():
    print('''
        MEMCACHE BACKUP TOOL
        Usage app.py -h 127.0.0.1 -p 11211 -n mem.mem -pa . -t 1 
            -h --mem_host: memcahe server host address
            -p --mem_port: memcache server port 
            -n --name: backup file name
            -pa --path: backup file path
            -t --type: operation 
                                1 : backup data to local file
                                2 : backup data to remote memcache server
                                3 : backup data to remote memcache server by local file
            -bh --backup_host: backup host address
            -bp --backup_port: backup port 

        ''')


def createSocket(socket_type, backup_host='127.0.0.1', backup_port=11211):
    socket.setdefaulttimeout(time_out)
    if socket_type == 1:
        global client
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((mem_host, mem_port))
    else :
        global rm_server
        rm_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rm_server.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536) # restore redhat6 python2.6.6 send data error
        rm_server.connect((backup_host, backup_port))

def getMemInfo():
    tmp = re.compile(r'\S+\s\d+').findall(getResponse(CMD_STATS).decode())
    if not tmp:
        raise Exception('get memcache info faild')
    response = {}
    for i in tmp:
        tmp_list = i.split(' ')
        response[tmp_list[0]] = tmp_list[1]
    return response

# {items_id : item_num,...}
def getItems():
    tmp = re.compile(r'items:\d+:number\s\d+').findall(getResponse(CMD_STATS_ITEMS).decode())
    if not tmp:
        raise Exception('get items info faild')
    response = []
    for i in tmp:
        tmp_list = i.split(':')
        response.append([tmp_list[1], tmp_list[-1].split(' ')[1]])
    return response

# [[key_name, key_size, time],...]
def getKeys(*item_info):
    tmp = re.compile(r'ITEM\s.+\s\[.+\]').findall(getResponse((cmd_cachedump.format(*item_info)).encode()).decode())
    key_info_list = []
    for i in tmp:
        # print(i)
        tmp_key_info = i.split(' ')
        key_info_list.append([tmp_key_info[1], tmp_key_info[2][1:], tmp_key_info[-2]])
    return key_info_list

# <del>[key_name, data_size, time, flags, data]</del>
'''
value key_name flags 
'''
# [key_name, flags, time, data_size, data]
def getData(key_info):
    tmp_response = getResponse(cmd_get.format(key_info[0]).encode())
    if len(tmp_response) > DATA_FOOT_LEN + DATA_HEAD_LEN:
        data_index = -int(key_info[1]) - DATA_SUCCESS_FOOT_LEN
        try:
            tmp_list = [key_info[0],
            int((tmp_response[DATA_HEAD_LEN + len(key_info[0]) + 2 : data_index - 1 - DATA_BREAK_LEN - len(key_info[1])]).decode()),
            key_info[2],
            key_info[1],
            tmp_response[data_index : -DATA_SUCCESS_FOOT_LEN]]
            
            if checkCacheData(tmp_list):
                return tmp_list
        except Exception as e:
            print(e, key_info[0])

def getResponse(cmd, pack_size = 1024):
    global client
    client.send(cmd)
    response = b''
    while True:
        data = client.recv(pack_size)
        response += data 
        if len(data) < pack_size and response[-DATA_FOOT_LEN:] == DATA_FOOT:
            return response

# param : key_name, flags, time, bytes, value
def pushCache(q, backup_host, backup_port):
    global push_count, push_count_success
    createSocket(2, backup_host, backup_port)
    push_count = 0
    push_count_success = 0
    while True:
        cache_data = q.get()
        if cache_data :
            if cache_data == 'finish':
                break
            push_count += 1
            rm_server.send(cmd_set.format(*cache_data[:-1]).encode()+cache_data[-1]+cmd_set_foot)
            try:
                response = rm_server.recv(64)
                if response in CMD_STORED:
                    push_count_success += 1
                else:
                    print(response)
            except Exception as e:
                print(e,cache_data[0])
    print('push items %d success %d' % (push_count, push_count_success))
    rm_server.close()

# check data and param    
def checkHostAndPort(host, port):
    return re.compile(r'(\d{1,3}\.){3}(\d{1,3})').match(host) and port > 0 and port < 65535

def checkCacheData(cache_data):
    return cache_data and int(cache_data[-2]) == len(cache_data[-1])
    

def checkBackupFile(file_path, file_name):
    if not file_path or file_path == '.':
        file_path = os.path.abspath('.')
    if not file_name:
        file_name = 'mem.mem'
    abs_path = splitTrans(os.path.join(file_path, file_name))
    if not os.path.isfile(abs_path):
        sys.exit() 
    return abs_path

def checkAndCreateFile(path, name):
    if not path or path == '.':
        path = os.path.abspath('.')
    if not os.path.exists(path):
        os.mkdir(path)
    if not os.path.isdir(path):
        print('warn: not found %s path, please change the backup path' % path)
        sys.exit()
    if not name:
        name = 'mem.mem'
    if name in os.listdir(path):
        print('warn: the file name %s already used, please set other name for file' % name)
        sys.exit()
    path = os.path.abspath(path)
    return splitTrans(os.path.join(path, name))


# utils
# translate split char by os 
def splitTrans(param):
    print(param)
    if param:
        if os.name == 'posix':
            return param.replace('\\', '/')
        return param.replace('/', '\\')    


# read and write

# get data with queue and write file 
def writeFile(q, file_path):
    data_count = 0
    with open(file_path, 'wb') as backup_file:
        while True :
            cache_item = q.get()
            if cache_item == 'finish':
                break
            if cache_item:
                pickle.dump(cache_item, backup_file)
                data_count += 1
    print('save cache data total:', data_count)

# read data by file and put queue 
def readFile(q, file_path):
    print(file_path)
    with open(file_path, 'rb') as backup_file:
        try:
            while True:
                tmp = pickle.load(backup_file)
                if tmp:
                    q.put(tmp)
        except Exception:
            q.put('finish')
            # pickle Exception in over 


# backup to remote server by file
def backupMemServer(q):
    try: 
        createSocket(1)
        mem_curr_time = int(getMemInfo()['time'])
        mem_max_time = mem_curr_time + 60 * 60 *24 *30
        cache_items = getItems()
        items_size = len(cache_items)
        items_count = 0
        cache_data = 0
        for curr_item in cache_items:
            keys = getKeys(*curr_item)
            keys_size = curr_item[1]
            key_count = 0
            for curr_key in keys:
                tmp_data = getData(curr_key)
           
                if tmp_data:
                    exp_time = tmp_data[2]
                    if int(exp_time) < mem_max_time:
                        tmp_data[2] = int(exp_time) - mem_curr_time
                    else:
                        tmp_data[2] = 0
                    q.put(tmp_data)
                    key_count += 1
                    cache_data += 1
            print('item %s number %s compile %s' % (curr_item[0], curr_item[1], key_count))
            items_count += 1
        total_cache_data = sum([int(v[1]) for v in cache_items])
        print('items total %s compile %s , total cache data %s compile %s' % (items_size, items_count, total_cache_data, cache_data))
        q.put('finish')
    except Exception as e:
        raise e
    finally:
        q.put('finish')
    


if __name__ == '__main__':
    argv = sys.argv
    argv_len = len(sys.argv[1:])
    if argv_len == 0 or argv_len % 2 != 0:
        usage()
        sys.exit()
    for i in range(0, argv_len, 2):
        cmd_head = argv[1 + i]
        if cmd_head in ('-h', '--mem_host') :
            mem_host = argv[2+i]
        elif cmd_head in ('-p', '--port'):
            mem_port = int(argv[2+i])
        elif cmd_head in ('-n', '--name'):
            file_name = argv[2+i]
        elif cmd_head in ('-bh', '--backup_host'):
            backup_host = argv[2+i]
        elif cmd_head in ('-bp', '--backup_port'):
            backup_port = int(argv[2+i])
        elif cmd_head in ('-t', '--type'):
            # operat_type 1 backup to local file 2 backup to remote 3 restore to remote by local file
            operat_type = argv[2+i]
        elif cmd_head in ('-pa', '--path'):
            file_path = argv[2+i]
    q = Queue()
    pp = None
    pw = None
    client = None
    rm_server = None
    try:
        if not operat_type:
            print('error: use -t or --type set your operation')
            sys.exit()
        if '1' == operat_type:
            file_add = checkAndCreateFile(file_path, file_name)
            pw = Process(target=writeFile, args=(q, file_add))
            pw.start()
            backupMemServer(q)
            pw.join()
        elif '2' == operat_type:
            pp = Process(target=pushCache, args=(q, backup_host, backup_port))
            pp.start()
            backupMemServer(q)
            pp.join()
        elif '3' == operat_type:
            file_add = checkBackupFile(file_path, file_name)
            pp = Process(target=pushCache, args=(q, backup_host, backup_port))
            pp.start()
            readFile(q, file_add)
            pp.join()
    except Exception as e:
        print(e)
    finally:
        if client:
            client.close()
        if rm_server:
            rm_server.close()
        if pw:
            pw.join()
        if pp:
            pp.join()
        