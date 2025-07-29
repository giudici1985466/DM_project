import pandas as pd
import numpy as np
from pathlib import Path

class ETLTransformation:
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _read_csv(self, filename: str) -> pd.DataFrame:
        #reads csv from the input directory
        return pd.read_csv(self.input_dir / filename)

    def _write_csv(self, df: pd.DataFrame, output_filename: str) -> None:
        #writes a dataframe to a csv in the output directory
        df.to_csv(self.output_dir / output_filename, index=False)

    def constructor_results_processing (self) -> None:
        #read the file constructor_result.csv
        df = self._read_csv("constructor_results.csv")

        #remove the unnecessary columns, status
        unnecessary_col = ["status"]
        df = df.drop(columns = unnecessary_col)
        #check for duplicate
        df = df.drop_duplicates()
        #conversions of the column point from float to integer
        df['points'] = df['points'].round().astype('Int64')
        #columns renaming
        rename_map = {
            'constructorResultsId': 'constructors_results_id',
            'raceId': 'race_id',
            'constructorId': 'constructor_id',
            'points': 'points'
        }
        df = df.rename(columns=rename_map)
        #columns reording
        correct_order = ['constructors_results_id', 'race_id', 'constructor_id', 'points']
        df = df[correct_order]
        #save the results
        self._write_csv(df, "constructor_results_staging.csv")
    
    def constructor_standings_processing (self) -> None:
        #read the file constructor_standings.csv
        df = self._read_csv("constructor_standings.csv")
        #remove the unnecessary columns, status
        unnecessary_col = ["positionText"]
        df = df.drop(columns = unnecessary_col)
        #check for duplicate
        df = df.drop_duplicates()
        #conversions of the column point from float to integer
        df['points'] = df['points'].round().astype('Int64')
        #renaming
        rename_map = {
            'constructorStandingsId': 'constructors_standings_id',
            'raceId': 'race_id',
            'constructorId': 'constructor_id',
            'points': 'points',
            'position': 'pos',
            'wins': 'wins'
        }
        df = df.rename(columns=rename_map)
        self._write_csv(df, "constructor_standings_staging.csv")
    
    def weather_processing (self) -> None:
        #read the file weather.csv
        df = self._read_csv("weather.csv")


        df=df.drop_duplicates()
        
        #split the field name into two distinct fields and overwrite date
        df[['date', 'hour']] = df['date'].str.split('T', expand=True)

 
        # Replace invalid wind_direction values with NaN
        df.loc[(df['wind_direction'] < 0) | (df['wind_direction'] > 360), 'wind_direction'] = np.nan


        # Replace invalid air_humidity values with NaN
        df.loc[(df['humidity'] < 0) | (df['humidity'] > 100), 'humidity'] = np.nan

        df['rainfall'] = pd.to_numeric(df['rainfall'], errors='coerce')
        df.loc[~df['rainfall'].isin([0, 1]), 'rainfall'] = np.nan



        #reorder the fields according to the order of the DB
        correct_order = ['date', 'hour', 'session_key', 'meeting_key', 'track_temperature', 'air_temperature', 'wind_direction', 'wind_speed', 'rainfall', 'humidity', 'pressure']
        df = df[correct_order]

        self._write_csv(df, "weather_staging.csv")

    
    
    def status_processing (self) -> None:
        
        df=self._read_csv("status.csv")

        df=df.drop_duplicates()

        rename_map={
            'statusId' : 'status_id'
        }

        df=df.rename(columns=rename_map)


        df['status_id'] = df['status_id'].astype('Int64')  
        df['status'] = df['status'].astype(str)



        self._write_csv(df, "status_staging.csv")

    def circuit_processing (self) -> None:

        df=self._read_csv("circuits.csv")

        

        unnecessary_col = ["circuitRef"]


        df=df.drop(columns=unnecessary_col)

        df=df.drop_duplicates()





        rename_map={
            'circuitId':'circuit_id'

        }
        
        df=df.rename(columns=rename_map)

    

        # Convert to numeric first (in case of strings or mixed types)
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
        df['alt'] = pd.to_numeric(df['alt'], errors='coerce')

        # Replace out-of-range values with NaN
        df.loc[(df['lat'] < -90) | (df['lat'] > 90), 'lat'] = np.nan
        df.loc[(df['lng'] < -180) | (df['lng'] > 180), 'lng'] = np.nan
        df.loc[(df['alt'] < -500) | (df['alt'] > 12000), 'alt'] = np.nan


        df['circuit_id'] = df['circuit_id'].astype('Int64')  # Nullable integer

        self._write_csv(df, "circuits_staging.csv")

    def countries_processing(self) -> None:

        df=self._read_csv("countries.csv")

        df=df.drop_duplicates()

        self._write_csv(df, "countries_staging.csv")










       

