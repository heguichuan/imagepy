# import ffmpeg

# info = ffmpeg.probe("./4.MP4")
# print(info['format']['tags'].get('creation_time', '2'))
# print(info)


from hachoir import parser,metadata
parserFile = parser.createParser('./51.MOV') #解析文件
metadataDecode = metadata.extractMetadata(parserFile) # 获取文件的metadata
print((metadataDecode))