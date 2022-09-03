import os
import patoolib
import glob
import pandas as pd
from tqdm import tqdm
import jdatetime
from jdatetime import date
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
        self.files = []
        for root, dirs, files in os.walk("./"):
            for file in files:
                if file.endswith(".xlsx") and (
                    (file.startswith("Hourly") and self.hourly)
                    or (file.startswith("Daily") and not self.hourly)
                ):
                    self.files.append(os.path.join(root, file))
        # print(self.files)
        df = pd.DataFrame()
        for f in tqdm(self.files, desc="Loading"):
            data = pd.read_excel(f, "Sheet1", header=1)
            cols = data.columns
            cols_normalized = [
                self.normalizer.normalize(col.replace("غیرمجاز", "غیر مجاز"))
                for col in cols
            ]
            data.columns = cols_normalized
            df = pd.concat([df, data], ignore_index=True)
        return df

    def process_df(self, df: pd.DataFrame):
        delete_columns = ["زمان پایان", "زمان شروع", "نام محور"]
        for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing"):
            holiday = False
            _date, time = row["زمان شروع"].split(" ")
            if _date in self.holidays:
                holiday = True
            df.loc[idx, "holiday"] = holiday
            year, month, day = _date.split("/")
            if day.startswith("0"):
                day = day[1]
            if month.startswith("0"):
                month = month[1]
            d = date(int(year), int(month), int(day), locale="fa_IR")
            df.loc[idx, "date"] = d.strftime("%Y-%m-%d")
            df.loc[idx, "day_of_week"] = str(d.weekday())  # Shanbeh: 0
            df.loc[idx, "time_start"] = time
            if row["کد محور"] not in self.roads:
                self.roads[row["کد محور"]] = row["نام محور"]
        with open("data/roads.pkl", "wb") as f:
            pickle.dump(self.roads, f)
        for col in delete_columns:
            df = df.drop(col, axis=1)
        df = df.sort_values(("date"))
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
    df = loader.create_df_from_xlsx()
    df = loader.process_df(df)
    loader.save_df(df)
    print(loader.load_df().shape)
