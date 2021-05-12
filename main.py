# /usr/bin/env python
# -*- coding:utf-8 -*-

# https://stackoverflow.com/questions/7829499/using-hashlib-to-compute-md5-digest-of-a-file-in-python-3
# https://www.cnblogs.com/ivictor/p/4377609.html
# https://zhuanlan.zhihu.com/p/67959761
# https://www.huaweicloud.com/articles/b4f513903329669a822a4414d67b19a0.html
# https://docs.python.org/zh-cn/3/library/multiprocessing.html?highlight=queue
# https://pypi.org/project/ExifRead/
# https://www.jianshu.com/p/3b61923efdf1
# https://blog.csdn.net/weixin_43745169/article/details/100988915
# https://stackoverflow.com/questions/10075115/call-exiftool-from-a-python-script

import exifread
import os
import shutil
import hashlib
from functools import partial
from multiprocessing import Pool, Manager
import glob
from hachoir import parser,metadata
from geopy.geocoders import BaiduV3
from configparser import ConfigParser
import ffmpeg # pip install ffmpeg-python
import subprocess
import json
import time

# 自定义程序运行参数
mime_types = ['jpeg', 'png', 'jpg', 'webp', 'gif', 'bmp', 'mp4', 'mov', 'm4v', 'wmv', 'avi', 'rm', 'rmvb']
root = os.getcwd()
dist_root = os.path.join(root, 'dist')
no_create_time_root = os.path.join(root, '无拍摄时间')
log_file = os.path.join(root, '重复文件记录.txt')
config_path = os.path.join(root, 'config.ini')
exiftool_bin = '/usr/local/bin/exiftool'
###################################################################

geolocator = BaiduV3(
    api_key='DFUEvQuBiq59GKd1S6GHdK2MoaSZkELF',
    security_key='6xO3d0VszSBKLBNpaO7XI9gHrTPexat6',
)

def parse_config():
    global mime_types,root,dist_root,no_create_time_root,log_file
    if not os.path.exists(config_path):
        return
    cfg = ConfigParser()
    try:
        cfg.read('./config.ini')
    except:
        return
    try:
        mime_types = cfg['common']['mime_types'].split(',')
    except:
        pass
    try:
        root = cfg['common']['root']
    except:
        pass
    try:
        dist_root = cfg['common']['dist']
    except:
        pass
    try:
        no_create_time_root = cfg['common']['fallback_dist']
    except:
        pass
    try:
        log_file = cfg.get('common', 'log_file')
    except:
        pass

def ensure_path():
    global mime_types
    mime_types.extend(list(map(lambda s: s.upper(), mime_types)))
    if not os.path.exists(no_create_time_root):
        os.mkdir(no_create_time_root)
    if not os.path.exists(dist_root):
        os.mkdir(dist_root)

def get_images_path(media_type):
    pattern = os.path.join(root, '**/*.' + media_type)
    return glob.glob(pattern, recursive=True)

def check_md5(f):
    d = hashlib.md5()
    for buf in iter(partial(f.read, 8 * 1024), b''):
        d.update(buf)
    return d

def video_meta(video_path):
    info = ffmpeg.probe(video_path)
    return info['format']['tags'].get('creation_time', '')

def output_duplicated_files(duplicated_files):
    if len(duplicated_files.values()) > 0:
        with open(log_file, 'w') as f:
            for item in duplicated_files.values():
                f.write(item.replace(',', '\n') +'\n\n')
        print('存在重复文件，详情查看：重复文件记录.txt')

def get_media_metas(filePath):
    parserFile = parser.createParser(filePath) #解析文件
    if not parserFile:
        return ''
    try:
        metadataDecode = metadata.extractMetadata(parserFile) # 获取文件的metadata
    except ValueError:
        return ''
    if not metadataDecode:
        return ''
    metaInfos = metadataDecode.exportPlaintext(line_prefix="") # 将文件的metadata转换为list,且将前缀设置为空
    creation_date = ''
    date_time_original = ''
    latitude = ''
    longitude = ''
    for meta in metaInfos:
        #如果字符串在列表中,则提取数字部分,即为文件创建时间
        if 'Creation date' in meta:
            creation_date = meta.replace('Creation date: ', '').replace(' ', '_').replace(':', '')
        elif 'Date-time original' in meta:
            date_time_original = meta.replace('Date-time original: ', '').replace(' ', '_').replace(':', '')
        elif 'Latitude' in meta:
            latitude = meta.replace('Latitude: ', '')
        elif 'Longitude' in meta:
            longitude = meta.replace('Longitude: ', '')
    geo_address = geo_parse(latitude, longitude)
    return ((date_time_original or creation_date), geo_address)

class ExifTool(object):
    # windows: sentinel = "{ready}\r\n" #todo
    sentinel = "{ready}\n"

    def __init__(self, executable=exiftool_bin):
        self.executable = executable

    def __enter__(self):
        self.process = subprocess.Popen(
            [self.executable, "-stay_open", "True",  "-@", "-"],
            universal_newlines=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        return self

    def  __exit__(self, exc_type, exc_value, traceback):
        self.process.stdin.write("-stay_open\nFalse\n")
        self.process.stdin.flush()

    def execute(self, *args):
        args = args + ("-execute\n",)
        self.process.stdin.write(str.join("\n", args))
        self.process.stdin.flush()
        output = ""
        fd = self.process.stdout.fileno()
        while not output.endswith(self.sentinel):
            output += os.read(fd, 4096).decode('utf-8')
        return output[:-len(self.sentinel)]

    def get_metadata(self, *filenames):
        return json.loads(self.execute("-G", "-j", "-n", *filenames))

def create_date_parse(create_date):
    create_date = create_date.replace(' ', '').replace(':', '')
    dist_path_time_part = ''
    if create_date != '':
        year_month = create_date[:6]
        year = year_month[:4]
        # 如果年份小于2000年明显不对
        if int(year) > 2000:
            tmp = list(create_date)
            tmp.insert(8, '_')
            dist_path_time_part = os.path.join(year, year_month, ''.join(tmp))
    return dist_path_time_part

def parse_meta(meta):
    create_date_keys = ('EXIF:CreateDate', 'QuickTime:CreateDate')
    create_date = ''
    for key in create_date_keys:
        if meta.get(key):
            create_date = meta.get(key)
            break
    return (create_date_parse(create_date), meta.get('SourceFile', ''))

def check_md5_and_move_file(source_file, dist_path_time_part, duplicated_files, lock):
    with open(source_file, 'rb') as f:
        md5 = check_md5(f)
    file_hash = md5.hexdigest()[8:24]
    dist_file_path = ''
    if not dist_path_time_part == '':
        dist_file_path = os.path.join(dist_root, dist_path_time_part + '_' + file_hash + os.path.splitext(source_file)[1])
    else:
        dist_file_path = os.path.join(no_create_time_root, file_hash + os.path.splitext(source_file)[1])
    lock.acquire()
    try:
        if not os.path.exists(dist_file_path):
            # 判断目标目录是否存在，不存在则创建，不然move操作报错
            dirname = os.path.dirname(dist_file_path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            shutil.move(source_file, dist_file_path)
        else:
            if file_hash in duplicated_files:
                duplicated_files[file_hash] = duplicated_files[file_hash] + ',' + source_file
            else:
                duplicated_files[file_hash] = dist_file_path + ',' + source_file
    except:
        print('文件移动失败：' + source_file + ' -> ' + dist_file_path)
    lock.release()

def parse_metas(metas):
    return map(parse_meta, metas)

def travel_files(path, record_files, duplicated_files, lock):
    with open(path, 'rb') as f:
        md5 = check_md5(f)
    unique_key = (os.path.getsize(path), md5.hexdigest())

    lock.acquire()
    if unique_key in record_files:
        if unique_key in duplicated_files:
            duplicated_files[unique_key] = duplicated_files[unique_key] + ',' + path
        else:
            duplicated_files[unique_key] = record_files[unique_key] + ',' + path
        lock.release()
        return
    record_files[unique_key] = '**-' + unique_key[1][8:24] + os.path.splitext(path)[1]
    lock.release()
    # todo 不查md5，使用创建时间\文件大小\gps\来决定唯一性。对于没有创建时间的再计算md5


def move_file(path, record_files, duplicated_files, lock):
    with open(path, 'rb') as f:
        md5 = check_md5(f)
    unique_key = (os.path.getsize(path), )

    lock.acquire()
    if unique_key in record_files:
        if unique_key in duplicated_files:
            duplicated_files[unique_key] = duplicated_files[unique_key] + ',' + path
        else:
            duplicated_files[unique_key] = record_files[unique_key] + ',' + path
        lock.release()
        return
    record_files[unique_key] = '**-' + unique_key[1][8:24] + os.path.splitext(path)[1]
    lock.release()
    (create_time, geo_address) = get_media_metas(path)
    if geo_address != '':
        geo_address = '_' + geo_address
    dist_dir = no_create_time_root
    filename_prefix = unique_key[1][8:24]
    if create_time != '':
        year_month = create_time[:7]
        year = year_month[:4]
        # 如果年份小于2000年明显不对
        if int(year) > 2000:
            filename_prefix = create_time + '_' + unique_key[1][8:24]
            dist_dir = os.path.join(dist_root, year + '/' + year_month)
            if not os.path.exists(dist_dir):
                os.makedirs(dist_dir)
    
    dist_path = os.path.join(dist_dir, filename_prefix + geo_address + os.path.splitext(path)[1])
    if not os.path.exists(dist_path):
        shutil.move(path, dist_path)
    else:
        # 可能之前该目录已经存在被处理的文件
        lock.acquire()
        if unique_key in duplicated_files:
            duplicated_files[unique_key] = duplicated_files[unique_key] + ',' + path
        else:
            duplicated_files[unique_key] = record_files[unique_key] + ',' + path
        lock.release()

parse_config()
ensure_path()

if __name__=="__main__":
    print('开始处理...')
    cpu_count = os.cpu_count()
    images = []
    for t in mime_types:
        images.extend(get_images_path(t))

    with ExifTool() as ex, Manager() as manager:
        pointer = 0
        skip = 1

        duplicated_files = manager.dict() #保存重复的文件信息
        lock = manager.Lock()
        pool = Pool(cpu_count - 1 if cpu_count - 1 > 1 else 1)
        while pointer < len(images):
            pointer += skip
            metas = ex.get_metadata(*images[pointer-skip:pointer])
            for (dist_path_time_part, source_file) in list(parse_metas(metas)):
                pool.apply_async(check_md5_and_move_file, args=(source_file, dist_path_time_part, duplicated_files, lock))
        pool.close()
        pool.join()
        output_duplicated_files(duplicated_files)
    print('处理完成啦!')
