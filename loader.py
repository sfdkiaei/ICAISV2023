import os
import patoolib
import glob

class Loader:
    def __init__(self) -> None:
        pass
    
    def extract(self):
        files = glob.glob('data/*.rar')
        for file in files:
            outputpath = os.path.join(file.split('.')[0])
            if not os.path.exists(outputpath):
                os.makedirs(outputpath)
                patoolib.extract_archive(file, outdir=outputpath)
                
    def main(self):
        pass
        
if __name__ == '__main__':
    loader = Loader()
    loader.main()
