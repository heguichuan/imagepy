# import ffmpeg

# info = ffmpeg.probe("./1.mov")
# # print(info['format']['tags'].get('creation_time', '2'))
# print(info)


# from hachoir import parser,metadata
# parserFile = parser.createParser('./51.MOV') #解析文件
# metadataDecode = metadata.extractMetadata(parserFile) # 获取文件的metadata
# print((metadataDecode))


# import subprocess
# import os
# import json

# class ExifTool(object):

#     sentinel = "{ready}\r\n"

#     def __init__(self, executable="/usr/local/bin/exiftool"):
#         self.executable = executable

#     def __enter__(self):
#         self.process = subprocess.Popen(
#             [self.executable, "-stay_open", "True",  "-@", "-"],
#             universal_newlines=True,
#             stdin=subprocess.PIPE, stdout=subprocess.PIPE)
#         return self

#     def  __exit__(self, exc_type, exc_value, traceback):
#         self.process.stdin.write("-stay_open\nFalse\n")
#         self.process.stdin.flush()

#     def execute(self, *args):
#         args = args + ("-execute\n",)
#         self.process.stdin.write(str.join("\n", args))
#         self.process.stdin.flush()
#         output = ""
#         fd = self.process.stdout.fileno()
#         while not output.endswith(self.sentinel):
#             output += os.read(fd, 4096).decode('utf-8')
#         return output[:-len(self.sentinel)]

#     def get_metadata(self, *filenames):
#         return json.loads(self.execute("-G", "-j", "-n", *filenames))

# if __name__=="__main__":
#     with ExifTool() as e:
#         metadata = e.get_metadata('./1.mov', './2.mp4')
#         print(metadata)
#         print('-------------------------')
#         metadata = e.get_metadata('./12.jpeg')
#         print(metadata)

# import exifread
import os
print(os.path.dirname('./df/dfdf/er/a'))

# with open('./2.mp4', 'rb')
