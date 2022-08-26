import os
import patoolib
import glob
import pandas as pd
from tqdm import tqdm

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
                
    def create_df_from_xlsx(self):
        files = glob.glob('data/*/*/‫حجم تردد ساعتی‬/*.xlsx')
        df = pd.DataFrame()
        for f in tqdm(files):
            data = pd.read_excel(f, 'Sheet1', header=1)
            df = pd.concat([df, data])
        return df
    
    def save_df(self, df: pd.DataFrame):
        df.to_pickle('data/data.pkl')
        
    def load_df(self) -> pd.DataFrame:
        return pd.read_pickle('data/data.pkl')
        
        
        
if __name__ == '__main__':
    loader = Loader()
    df = loader.create_df_from_xlsx()
    loader.save_df(df)
    print(loader.load_df().shape)
