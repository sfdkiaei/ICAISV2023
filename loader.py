import os
import patoolib
import glob
import pandas as pd
from tqdm import tqdm
import jdatetime
import pickle
from hazm import Normalizer


class Loader:
    def __init__(self, hourly: bool = False) -> None:
        self.hourly = hourly
        df_holidays = pd.read_csv("data/holidays/holidays.csv")
        self.holidays = []
        for idx, row in df_holidays.iterrows():
            self.holidays.append(row["jalali_date"].replace("-", "/"))
        self.normalizer = Normalizer()
        self.roads = {}

    def extract(self):
        files = glob.glob("data/*.rar")
        for file in files:
            outputpath = os.path.join(file.split(".")[0])
            if not os.path.exists(outputpath):
                os.makedirs(outputpath)
                patoolib.extract_archive(file, outdir=outputpath)

    def create_df_from_xlsx(self):
        if self.hourly:
            files = glob.glob("data/*/*/‫حجم تردد ساعتی‬/*.xlsx")
        else:
            files = glob.glob("data/*/*/‫حجم تردد روزانه‬/*.xlsx")
        df = pd.DataFrame()
        for f in tqdm(files, desc="Loading"):
            data = pd.read_excel(f, "Sheet1", header=1)
            cols = data.columns
            cols_normalized = [
                self.normalizer.normalize(col.replace("غیرمجاز", "غیر مجاز"))
                for col in cols
            ]
            data.columns = cols_normalized
            df = pd.concat([df, data])
        return df

    def process_df(self, df: pd.DataFrame):
        delete_columns = ["زمان پایان", "زمان شروع", "نام محور"]
        for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing"):
            holiday = False
            date = row["زمان شروع"].split(" ")[0]
            time = row["زمان شروع"].split(" ")[1]
            if date in self.holidays:
                holiday = True
            df.loc[idx, "holiday"] = holiday
            year = int(date.split("/")[0])
            month = int(date.split("/")[1])
            day = int(date.split("/")[2])
            d = jdatetime.date(year, month, day)
            df.loc[idx, "date"] = d
            df.loc[idx, "day_of_week"] = str(d.weekday())  # Shanbeh: 0
            df.loc[idx, "time_start"] = time
            if row["کد محور"] not in self.roads:
                self.roads[row["کد محور"]] = row["نام محور"]
        with open("data/roads.pkl", "wb") as f:
            pickle.dump(self.roads, f)
        for col in delete_columns:
            df = df.drop(col, axis=1)
        return df

    def save_df(self, df: pd.DataFrame):
        df.to_pickle("data/data.pkl")

    def load_df(self) -> pd.DataFrame:
        return pd.read_pickle("data/data.pkl")

    def get_roads(self):
        with open("data/roads.pkl", "rb") as f:
            return pickle.load(f)


if __name__ == "__main__":
    loader = Loader()
    # df = loader.create_df_from_xlsx()
    # loader.save_df(df)
    # print(loader.load_df().shape)
