import glob
import io
import os
import zipfile

class InputHandler:
    def __init__(self, input_path: str, encoding: str, target_ext: str):
        self.input_path = input_path
        self.encoding = encoding
        self.target_ext = target_ext
        self.dir_mode, self.zip_mode = False, False
        if os.path.isdir(input_path):
            self.dir_mode = True
        elif zipfile.is_zipfile(input_path):
            self.zip_mode = True

    def get_file_list(self) -> list:
        if self.dir_mode:
            glob_expr = f'{self.input_path}/**/*{self.target_ext}'
            file_list = glob.glob(glob_expr, recursive=True)
        elif self.zip_mode:
            with zipfile.ZipFile(self.input_path, 'r') as zip_obj:
                zip_content = zip_obj.namelist()
            file_list = [ f for f in zip_content if f.endswith(self.target_ext) ]
        else:
            print('Error: no valid input found')
            file_list = []
        return file_list

    def get_file(self, filepath: str) -> str:
        try:
            if self.dir_mode:
                with open(filepath, 'r', encoding=self.encoding) as f:
                    return f.read()
            else:
                with zipfile.ZipFile(self.input_path, 'r') as zip_obj:
                    with zip_obj.open(filepath, 'r') as f:
                        return io.TextIOWrapper(f, self.encoding).read()
        except Exception as e:
            print(f'Error reading file: {e}')
            return ''
