import os
import exifread
import time
import re
from hachoir import parser,metadata

def decodeMediaFile(filePath):
    parserFile = parser.createParser(filePath) #解析文件
    if not parserFile:
        return ''
    try:
        metadataDecode = metadata.extractMetadata(parserFile) # 获取文件的metadata
    except ValueError:
        return ''
    if not metadataDecode:
        return ''

    print(metadataDecode)
    metaInfos = metadataDecode.exportPlaintext(line_prefix="") # 将文件的metadata转换为list,且将前缀设置为空
    print(metaInfos)
    creation_date = ''
    date_time_original = ''
    for meta in metaInfos:
        #如果字符串在列表中,则提取数字部分,即为文件创建时间
        if 'Creation date' in meta:
            creation_date = re.sub(r"\D",'',meta)    #使用正则表达式将列表中的非数字元素剔除
        elif 'Date-time original' in meta:
            date_time_original = re.sub(r"\D",'',meta)    #使用正则表达式将列表中的非数字元素剔除
    return (date_time_original or creation_date)[0:8]

print(decodeMediaFile('./IMG_2617.mov'))