# -*- coding: utf-8 -*-
import os
import traceback
import gzip
import shutil


class JHWrite(object):
    out_file = {}

    def __init__(self, filename, line, mode="w"):
        self.write(filename, line, mode)

    @staticmethod
    def _gzip(sourcepath, destpath=None):
        destpath = sourcepath + ".gz" if destpath is None else destpath
        with open(sourcepath, 'rb') if not sourcepath.endswith(".gz") \
                else gzip.open(sourcepath, 'rb') as f_in, \
                gzip.open(destpath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            os.remove(sourcepath)

    @staticmethod
    def fileobj(filepath, mode="rb"):
        return open(filepath, mode) if not filepath.endswith(".gz") \
            else gzip.open(filepath, mode)

    @staticmethod
    def combinefiles(files_path, destfile_path):
        from os import path, sys
        dirname = path.dirname(destfile_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        destfile = JHWrite.fileobj(destfile_path, "w")
        map(lambda file: shutil.copyfileobj(file, destfile), [JHWrite.fileobj(filepath) for filepath in files_path
                                                              if os.path.exists(filepath)])
        destfile.close()

    @staticmethod
    def write(filename, line, mode="w"):
        filename = filename.replace("\\", "/")
        os.sep = "/"
        path, file = filename.rsplit(os.sep, 1)[0], filename.rsplit(os.sep, 1)[1]
        targetFilename, targetFileExtension = file.rsplit(".", 1)[0], file.rsplit(".", 1)[1]
        assert mode == "w" or mode == "a", "access file mode error!"
        if " " in path:
            return
        if not os.path.exists(path):
            os.makedirs(path)
        if targetFileExtension == "gz":
            out = JHWrite.out_file[filename] if JHWrite.out_file.get(filename, None) else gzip.open(filename, mode)
        else:
            out = JHWrite.out_file[filename] if JHWrite.out_file.get(filename, None) else open(filename, mode)
        JHWrite.out_file.setdefault(filename, out)
        out.write(line.strip() + os.linesep)

    @staticmethod
    def finished(iszip=False):
        for filename in JHWrite.out_file.keys():
            try:
                JHWrite.out_file[filename].close()
                if iszip and not filename.endswith(".gz"):
                    JHWrite._gzip(filename)
            except:
                print(traceback.print_exc())
            try:
                # 删除完成的文件路径（必须）
                del JHWrite.out_file[filename]
            except:
                print(traceback.print_exc())

if __name__ == "__main__":
    print(len(os.linesep), os.linesep)
    # JHWrite._gzip(r"F:/testfile/a.txt.gz", r"F:/testfile/a_0.txt.gz")
    JHWrite.fileobj(r"F:/testfile/c.txt.gz", "w")
    # for item in range(20, 30):
    #     item = str(item)
    #     JHWrite.write(r"F:/testfile/a.txt.gz", item, "a")
    # JHWrite.finished(True)
    # file = gzip.open()
    # file.write()