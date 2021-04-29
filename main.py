# /usr/bin/env python
# -*- coding:utf-8 -*-

# https://stackoverflow.com/questions/7829499/using-hashlib-to-compute-md5-digest-of-a-file-in-python-3
# https://www.cnblogs.com/ivictor/p/4377609.html
# https://zhuanlan.zhihu.com/p/67959761
# https://www.huaweicloud.com/articles/b4f513903329669a822a4414d67b19a0.html
# https://docs.python.org/zh-cn/3/library/multiprocessing.html?highlight=queue
# https://pypi.org/project/ExifRead/
import exifread
import os
import shutil
import hashlib
from functools import partial
from multiprocessing import Pool, Manager
import glob

root = os.getcwd()
dist_root = os.path.join(root, 'dist')
no_create_time_root = os.path.join(root, '无拍摄时间')
if not os.path.exists(no_create_time_root):
    os.mkdir(no_create_time_root)
if not os.path.exists(dist_root):
    os.mkdir(dist_root)

def get_images_path(media_type, images, lock):
    pattern = os.path.join(root, '**/*.' + media_type)
    images_part = glob.glob(pattern, recursive=True)
    lock.acquire()
    images.extend(images_part)
    lock.release()

def check_md5(path, f):
    d = hashlib.md5()
    for buf in iter(partial(f.read, 8 * 1024), b''):
        d.update(buf)
    return d

def output_duplicated_files(duplicated_files):
    if len(duplicated_files.values()) > 0:
        with open('./重复文件记录.txt', 'w') as f:
            for item in duplicated_files.values():
                f.write(item.replace(',', '\n') +'\n\n')
        print('存在重复文件，详情查看：重复文件记录.txt')

def move_file(path, record_files, duplicated_files, lock):
    with open(path, 'rb') as f:
        md5 = check_md5(path, f)
        tags = exifread.process_file(f, stop_tag='DateTimeOriginal', details=False)

    unique_key = (os.path.getsize(path), md5.hexdigest())
    lock.acquire()
    if unique_key in record_files:
        if unique_key in duplicated_files:
            duplicated_files[unique_key] = duplicated_files[unique_key] + ',' + path
        else:
            duplicated_files[unique_key] = record_files[unique_key] + ',' + path
        lock.release()
        return

    create_time = tags.get('EXIF DateTimeOriginal') or tags.get('Image DateTime')
    dist_dir = no_create_time_root
    filename_prefix = unique_key[1][8:24] + '-'
    if create_time != None:
        year_month_day = str(create_time)[0:10].replace(':', '-')
        year_month = year_month_day[0:7]
        year = year_month[0:4]
        filename_prefix = year_month_day.replace('-', '') + '-'
        dist_dir = os.path.join(dist_root, year + '/' + year_month)
    dist_path = os.path.join(dist_dir, filename_prefix + os.path.basename(path))

    record_files[unique_key] = dist_path
    lock.release()

    if create_time != None and not os.path.exists(dist_dir):
        os.makedirs(dist_dir)
    shutil.move(path, dist_path)

if __name__=="__main__":
    print('开始处理...')
    cpu_count = os.cpu_count()
    images = []
    with Manager() as manager1:
        images_proxy = manager1.list()
        lock1 = manager1.Lock()
        pl1 = Pool(cpu_count - 1 if cpu_count - 1 > 1 else 1)
        for t in ('jpeg', 'png', 'jpg', 'mp4', 'mov'):
            pl1.apply_async(get_images_path, args=(t, images_proxy, lock1))
        pl1.close()
        pl1.join()
        images.extend(images_proxy)

    with Manager() as manager2:
        record_files = manager2.dict() #保存已记录的文件信息
        duplicated_files = manager2.dict() #保存重复的文件信息
        lock2 = manager2.Lock()

        pl2 = Pool(cpu_count - 1 if cpu_count - 1 > 1 else 1)
        for image_path in images:
            pl2.apply_async(move_file, args=(image_path, record_files, duplicated_files, lock2))
        pl2.close()
        pl2.join()
        output_duplicated_files(duplicated_files)

    print('处理完成啦!')
