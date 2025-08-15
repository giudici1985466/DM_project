import pandas as pd
import numpy as np
from pathlib import Path
from rapidfuzz import fuzz, process
from utility import convert_into_ms
from utility import split_and_clean

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

    def constructors_results_processing (self) -> None:
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
        self._write_csv(df, "constructors_results_staging.csv")
    
    def constructors_standings_processing (self) -> None:
        #read the file constructor_standings.csv
        df = self._read_csv("constructor_standings.csv")
        
        #remove the unnecessary columns, status
        unnecessary_col = ["positionText"]
        df = df.drop(columns = unnecessary_col)
        
        #check for duplicate
        df = df.drop_duplicates()
        
        #conversions of the column point from float to integer
        df['points'] = df['points'].round().astype('Int64')
        
        #renaming columns
        rename_map = {
            'constructorStandingsId': 'constructors_standings_id',
            'raceId': 'race_id',
            'constructorId': 'constructor_id',
            'points': 'points',
            'position': 'pos',
            'wins': 'wins'
        }
        df = df.rename(columns=rename_map)

        #save the results
        self._write_csv(df, "constructors_standings_staging.csv")

    def drivers_processing(self) -> None:
        #read the file
        df = self._read_csv("drivers.csv")

        #remove the unnecessary columns
        unnecessary_col = ["number", "nationality"]
        df = df.drop(columns = unnecessary_col)

        #renaming columns
        rename_map = {
            "driverId" : "driver_id",
            "driverRef" : "driver_ref",
            "code" : "code",
            "forename" : "forename",
            "surname" : "surname",
            "dob" : "dob",
            "url" : "url"
        }
        df = df.rename(columns=rename_map)

        #remove duplicates
        df = df.drop_duplicates()
        self._write_csv(df, "drivers_staging.csv")

    def race_results_processing(self) -> None:
        #read the file
        df = self._read_csv("results.csv")

        #remove unnecessary columns
        unnecessary_col = ["positionText", "positionOrder", "time","fastestLapSpeed"]
        df = df.drop(columns = unnecessary_col)

        #rename columns
        rename_map = {
            "resultId" : "result_id",
            "raceId" : "race_id",
            "driverId" : "driver_id",
            "constructorId" : "constructor_id",
            "number" : "num", 
            "grid" : "grid",
            "position" : "pos",
            "points" : "points",
            "laps" : "laps",
            "milliseconds" : "milliseconds", 
            "fastestLap" : "fastest_lap",
            "rank" : "rank",
            "fastestLapTime" : "fastest_lap_time",
            "statusId" : "status_id"
        }
        df = df.rename(columns=rename_map)

        #conversions of the column point from float to integer
        df['points'] = df['points'].round().astype('Int64')

        df["fastest_lap_time"] = df["fastest_lap_time"].apply(convert_into_ms)

        #remove duplicates
        df = df.drop_duplicates()
        self._write_csv(df, "race_results_staging.csv")

    def seasons_processing(self) -> None:
        df = self._read_csv("seasons.csv")
        #don't need to remove columns
        #remove duplicates
        df = df.drop_duplicates()
        #don't need rename nor reordering
        self._write_csv(df, "seasons_staging.csv")
    
    def weather_processing (self) -> None:
        
        #read the file weather.csv
        df = self._read_csv("weather.csv")
        df=df.drop_duplicates()
        
        # Split into date (YYYY-MM-DD) and time (HH:MM:SS)
        df[['date', 'hour']] = df['date'].str.split('T', expand=True)


        # Replace invalid wind_direction values with NaN
        df.loc[(df['wind_direction'] < 0) | (df['wind_direction'] > 360), 'wind_direction'] = np.nan
        df['wind_direction'] = df['wind_direction'].round().astype('Int64')
        
        # Replace invalid air_humidity values with NaN
        df.loc[(df['humidity'] < 0) | (df['humidity'] > 100), 'humidity'] = np.nan
        df['humidity'] = df['humidity'].round().astype('Int64')

        df['rainfall'] = pd.to_numeric(df['rainfall'], errors='coerce')
        df.loc[~df['rainfall'].isin([0, 1]), 'rainfall'] = np.nan
        df['rainfall'] = df['rainfall'].round().astype('Int64')
        
        #Load races_staging.csv to map meeting_key → race_id
        races = self._read_csv("races_staging.csv")

        # Keep only necessary columns for merging
        races = races[['race_id', 'meeting_key']]

        # Merge to bring in race_id
        df = df.merge(races, on='meeting_key', how='inner')

        # Drop meeting_key and reorder
        df = df.drop(columns=['meeting_key'])

        # Reorder columns for DB
        correct_order = ['date', 'hour', 'session_key', 'race_id', 'track_temperature', 'air_temperature',
                        'wind_direction', 'wind_speed', 'rainfall', 'humidity', 'pressure']
        df = df[correct_order]

        # Write result
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
        df['alt'] = df['alt'].round().astype('Int64')
        
        # Replace out-of-range values with NaN
        df.loc[(df['lat'] < -90) | (df['lat'] > 90), 'lat'] = np.nan
        df.loc[(df['lng'] < -180) | (df['lng'] > 180), 'lng'] = np.nan
        df.loc[(df['alt'] < -500) | (df['alt'] > 12000), 'alt'] = np.nan
        df['circuit_id'] = df['circuit_id'].astype('Int64')  # Nullable integer

        country_map = {
            'UK': 'United Kingdom',
            'USA': 'United States',
            'UAE': 'United Arab Emirates'
        }
        df['country'] = df['country'].replace(country_map)
        
        self._write_csv(df, "circuits_staging.csv")

    def countries_processing(self) -> None:
        df=self._read_csv("countries.csv")
        df=df.drop_duplicates()
        self._write_csv(df, "countries_staging.csv")
    
    def races_processing(self) -> None:
        
        # Load CSVs
        df_races = self._read_csv("races.csv")
        df_meetings = self._read_csv("meetings.csv")
        
        # Strip not needed columns
        df_races = df_races[["raceId","year","round","circuitId","name","date","url"]].copy()
        df_meetings = df_meetings[["meeting_key", "meeting_name", "year"]].copy()
        
        # Clean names
        df_races['name_clean'] = df_races['name'].str.lower().str.strip()
        df_meetings['meeting_name_clean'] = df_meetings['meeting_name'].str.lower().str.strip()
        
        # Ensure year columns are integers
        df_races['year'] = pd.to_numeric(df_races['year'], errors='coerce').astype('Int64')
        df_meetings['year'] = pd.to_numeric(df_meetings['year'], errors='coerce').astype('Int64')
        matched_keys = []
        THRESHOLD = 85
        for _, race_row in df_races.iterrows():
            year = race_row['year']
            race_name_clean = race_row['name_clean']
            meetings_in_year = df_meetings[df_meetings['year'] == year]

            if not meetings_in_year.empty:
                best_match = process.extractOne(
                    race_name_clean,
                    meetings_in_year['meeting_name_clean'],
                    scorer=fuzz.token_sort_ratio
                )
                if best_match and best_match[1] >= THRESHOLD:
                    matched_name = best_match[0]
                    matched_row = meetings_in_year[meetings_in_year['meeting_name_clean'] == matched_name]
                    meeting_key = matched_row.iloc[0]['meeting_key']
                    matched_keys.append(meeting_key)
                else:
                    matched_keys.append(pd.NA)
            else:
                matched_keys.append(pd.NA)
        df_races['meeting_key'] = matched_keys
        df_races.drop(columns=['name_clean'], inplace=True)
        
        #rename columns
        rename_map = {   
            "raceId" : "race_id",
            "circuitId" : "circuit_id",
            "driverId" : "driver_id"  
        }
        df_races = df_races.rename(columns=rename_map)
        
        self._write_csv(df_races, "races_staging.csv")

    def constructors_processing(self)-> None:

        df=self._read_csv("constructors.csv")

        df['constructorId'] = pd.to_numeric(df['constructorId'], errors='coerce').astype('Int64')


        unnecessary_col = ["constructorRef","nationality"]
        df=df.drop(columns=unnecessary_col)
        
        df=df.drop_duplicates()

        rename_map = {
            "constructorId" : "constructor_id"
        }

        df=df.rename(columns=rename_map)

        self._write_csv(df, "constructors_staging.csv")

    def sessions_processing(self)-> None:

        df=self._read_csv("sessions.csv")

        df['session_key'] = pd.to_numeric(df['session_key'], errors='coerce').astype('Int64')
        df['meeting_key'] = pd.to_numeric(df['meeting_key'], errors='coerce').astype('Int64')


        df= df[["meeting_key", "session_key", "session_name"]].copy()
        

        #Load races_staging.csv to map meeting_key → race_id
        races = self._read_csv("races_staging.csv")

        # Keep only necessary columns for merging
        races = races[['race_id', 'meeting_key']]

        # Merge to bring in race_id
        df = df.merge(races, on=['meeting_key'], how='inner')

        # Drop meeting_key and reorder
        df = df.drop(columns=['meeting_key'])

        correct_order = ['session_key','race_id','session_name']
        df = df[correct_order]



        df=df.drop_duplicates()


        self._write_csv(df, "sessions_staging.csv")


    def sprint_results_preprocessing(self) -> None:
        #read the file
        df = self._read_csv("sprint_results.csv")

        #remove unnecessary columns
        unnecessary_col = ["positionText", "positionOrder", "time"]
        df = df.drop(columns = unnecessary_col)

        #rename columns
        rename_map = {
            "resultId" : "result_id",
            "raceId" : "race_id",
            "driverId" : "driver_id",
            "constructorId" : "constructor_id",
            "number" : "num", 
            "grid" : "grid",
            "position" : "pos",
            "points" : "points",
            "laps" : "laps",
            "milliseconds" : "milliseconds", 
            "fastestLap" : "fastest_lap",
            "rank" : "rank",
            "fastestLapTime" : "fastest_lap_time",
            "statusId" : "status_id"
        }
        df = df.rename(columns=rename_map)

        #conversions of the column point from float to integer
        df['points'] = df['points'].round().astype('Int64')

        df["fastest_lap_time"] = df["fastest_lap_time"].apply(convert_into_ms)

        #remove duplicates
        df = df.drop_duplicates()
        self._write_csv(df, "sprint_results_staging.csv")


    def drivers_standings_processing (self) -> None:
        #read the file constructor_standings.csv
        df = self._read_csv("driver_standings.csv")
        #remove the unnecessary columns, status
        unnecessary_col = ["positionText"]
        df = df.drop(columns = unnecessary_col)
        #check for duplicate
        df = df.drop_duplicates()
        #conversions of the column point from float to integer
        df['points'] = df['points'].round().astype('Int64')
        #renaming
        rename_map = {
            'driverStandingsId': 'drivers_standings_id',
            'raceId': 'race_id',
            'driverId': 'driver_id',
            'points': 'points',
            'position': 'pos',
            'wins': 'wins'
        }
        df = df.rename(columns=rename_map)
        self._write_csv(df, "drivers_standings_staging.csv")

    def driver_nationality_processing(self) -> None:
        df_drivers=self._read_csv("drivers.csv")
        df_countries=self._read_csv("countries.csv")


        df_drivers=df_drivers[["driverId","nationality"]].copy()
        df_countries=df_countries[["nationality"]].copy()


        df_countries['nationality_clean'] = df_countries['nationality'].str.lower().str.strip()
        reference_nationalities = df_countries['nationality_clean'].unique().tolist()

        THRESHOLD = 70                  #tune it to get the matching right
        matched_rows = []

        for _, row in df_drivers.iterrows():
            driver_id = row['driverId']
            nationality_parts = split_and_clean(row['nationality'])

            for nat in nationality_parts:
                match = process.extractOne(nat, reference_nationalities, scorer=fuzz.token_sort_ratio)
                if match and match[1] >= THRESHOLD:
                    # Get original form from countries
                    original_nat = df_countries[df_countries['nationality_clean'] == match[0]].iloc[0]['nationality']
                    matched_rows.append({'driverId': driver_id, 'nationality': original_nat})

                    
        output_df = pd.DataFrame(matched_rows)
        rename_map={
            "driverId" : "driver_id"
        }

        output_df = output_df.rename(columns=rename_map)
        self._write_csv(output_df, "driver_nationality_staging.csv")



   

    def constructor_nationality_processing(self) -> None:
        df_constructors=self._read_csv("constructors.csv")
        df_countries=self._read_csv("countries.csv")


        df_constructors=df_constructors[["constructorId","nationality"]].copy()
        df_countries=df_countries[["nationality"]].copy()


        df_countries['nationality_clean'] = df_countries['nationality'].str.lower().str.strip()
        reference_nationalities = df_countries['nationality_clean'].unique().tolist()

        THRESHOLD = 70                  #tune it to get the matching right
        matched_rows = []

        for _, row in df_constructors.iterrows():
            constructor_id = row['constructorId']
            nationality_parts = split_and_clean(row['nationality'])

            for nat in nationality_parts:
                match = process.extractOne(nat, reference_nationalities, scorer=fuzz.token_sort_ratio)
                if match and match[1] >= THRESHOLD:
                    # Get original form from countries
                    original_nat = df_countries[df_countries['nationality_clean'] == match[0]].iloc[0]['nationality']
                    matched_rows.append({'constructorId': constructor_id, 'nationality': original_nat})

                    
        output_df = pd.DataFrame(matched_rows)
        rename_map={
            "constructorId" : "constructor_id"
        }

        output_df = output_df.rename(columns=rename_map)
        self._write_csv(output_df, "constructor_nationality_staging.csv")

    def race_lineup_processing(self) -> None:
        #read the file
        df = self._read_csv("drivers_openf1.csv")

        #remove the unnecessary columnss
        unnecessary_col = ["broadcast_name", "country_code", "first_name","full_name", "headshot_url", "last_name", "name_acronym", "session_key", "team_name"]
        df.drop(columns = unnecessary_col)

       

        # read races_staging.csv to retrieve the race_id corresponding to the meeting_key
        races_df = self._read_csv("races_staging.csv")
        
        #remove unnecessary columns
        races_df = races_df[["race_id", "meeting_key"]]

        #merge between meeting_key and meeting_key of the two files
        df = df.merge(races_df, left_on="meeting_key", right_on="meeting_key", how="inner")

        #remove unnecessary columns
        df = df[["race_id", "driver_number", "team_colour"]]

        #read race_results_staging.cvs in order to obtain dirver_id
        races_staging_df = self._read_csv("race_results_staging.csv")

        #remove unnecessary columns
        races_staging_df = races_staging_df[["race_id", "driver_id", "num"]]

        df["driver_number"] = df["driver_number"].astype(int)
        races_staging_df["num"] = pd.to_numeric(races_staging_df["num"], errors="coerce").astype("Int64")
        #races_staging_df["num"] = races_staging_df["num"].astype(int)
        
        #merge between race_id, so that I can access the correct row
        df = df.merge(races_staging_df, left_on=["race_id", "driver_number"], right_on=["race_id", "num"], how = "inner")
        
        #remove unnecessary columns
        df = df[["race_id", "driver_id", "num", "team_colour"]]

        #renaming the columns
        rename_map = {
            "meeting_key" : "race_id",
            "num" : "driver_number",
            "team_colour" : "team_color",
            "driver_id" : "driver_id"
        }
        df = df.rename(columns = rename_map)
        #remove duplicates
        df = df.drop_duplicates(subset=["race_id", "driver_number"], keep="first")
        #columns reording
        correct_order = ['race_id', 'driver_id', 'driver_number', 'team_color']
        df = df[correct_order]

        #save the result
        self._write_csv(df, "race_lineup_staging.csv")
        
        
    def speed_processing(self) -> None:
        #read the file
        df = self._read_csv("speed_no_avg.csv")

        #remove duplicate
        df = df.drop_duplicates()

        #remove unnecessary columns
        unnecessary_col = ["year"]
        df = df.drop(columns=unnecessary_col)
    
        #replacing the meeting_key with the race_id
        races_df = self._read_csv("races_staging.csv")
        #removing the unnecessary columns
        races_df = races_df[["race_id", "meeting_key"]]

        #retrieve the race_id corresponding to the meeting_key
        df = df.merge(races_df, left_on="meeting_key", right_on="meeting_key", how = "inner")

        #remove unnecessary columns
        df = df[["race_id", "driver_number", "session_key", "lap_number", "st_speed"]]

        #renaming columns
        rename_map = {
            "race_id" : "race_id", 
            "session_key" : "session_key",
            "lap_number" : "lap_number",
            "st_speed" : "st_speed"
        }
        df = df.rename(columns = rename_map)

        #columns reordering
        correct_order = ['race_id', 'driver_number', 'session_key', 'lap_number', 'st_speed']
        df = df[correct_order]
        
        #save the results
        self._write_csv(df, "speed_staging.csv")

    def stints_processing(self) -> None:
        #read the file
        df = self._read_csv("stints.csv")

        #remove duplicates
        df = df.drop_duplicates()

        #remove unnecessary_columns
        unnecessary_col = ["year"]
        df = df.drop(columns=unnecessary_col)

        #reading the file races_staging.csv to retrieve the race_id corresponding to the meeting_key
        races_df = self._read_csv("races_staging.csv")

        #remove unnecessary columns
        races_df = races_df[["race_id", "meeting_key"]]

        #matching between meeting_key and meeting_key
        df = df.merge(races_df, left_on="meeting_key", right_on="meeting_key", how = "inner")

        #remove unnecessary columns
        df = df[["driver_number", "race_id", "stint_number", "compound", "lap_start", "lap_end", "session_key", "tyre_age_at_start"]]
        
        #converting lap_start and lap_end into integer
        df['lap_start'] = df['lap_start'].round().astype('Int64')
        df['lap_end'] = df['lap_end'].round().astype('Int64')
        #reordering columns
        correct_order = ["driver_number", "race_id", "stint_number", "compound", "lap_start", "lap_end", "session_key", "tyre_age_at_start"]
        df = df[correct_order]

        #save the results
        self._write_csv(df, "stints_staging.csv")

    def lap_times_processing(self) -> None:
        #read the file
        df = self._read_csv("lap_times.csv")

        #remove duplicates
        df = df.drop_duplicates()

        #remove unnecessary columns
        unnecessary_col = ["time"]
        df = df.drop(columns=unnecessary_col)

        #renaming columns
        rename_map = {
            "raceId" : "race_id",
            "driverId" :"driver_id",
            "position" : "pos", 
            "lap" : "lap_number"
        }
        df = df.rename(columns=rename_map)

        #reordering of the columns
        correct_order = ['race_id', 'driver_id', 'lap_number', 'pos', 'milliseconds']
        df = df[correct_order]

        #save the results
        self._write_csv(df, "lap_times_staging.csv")
        
    def pit_stops_processing(self) -> None:
        #read the file
        df = self._read_csv("pit_stops.csv")

        #remove duplicates
        df = df.drop_duplicates()

        #remove unnecessary columns
        unnecessary_col = ["time","duration"]
        df = df.drop(columns=unnecessary_col)

        #renaming columns
        rename_map = {
            "raceId" : "race_id",
            "driverId" : "driver_id", 
            "stop" : "stop",
            "lap" : "lap",
            "milliseconds" : "milliseconds"
        }
        df = df.rename(columns=rename_map)

        #reordering columns
        correct_order = ['race_id', 'driver_id', 'stop', 'lap', 'milliseconds']
        df = df[correct_order]

        #save the results
        self._write_csv(df, "pit_stops_staging.csv")                          

    def qualifying_processing(self) -> None:
        #read the file
        df = self._read_csv("qualifying.csv")

        #remove duplicates
        df = df.drop_duplicates()

        #remove unnecessary columns
        unnecessary_col = ["number"]
        df = df.drop(columns=unnecessary_col)

        #renaming columns
        rename_map = {
            "qualifyId" : "qualify_id",
            "raceId" : "race_id",
            "driverId" : "driver_id", 
            "constructorId" : "constructor_id",
            "position": "pos"
        }
        df = df.rename(columns=rename_map)

        #reordering columns
        correct_order = ['qualify_id', 'constructor_id', 'race_id', 'driver_id', 'pos', 'q1', 'q2', 'q3']
        df = df[correct_order]
        
        #conversion  of q1, q2 and q3 into ms
        df["q1"] = df["q1"].apply(convert_into_ms)
        df["q2"] = df["q2"].apply(convert_into_ms)
        df["q3"] = df["q3"].apply(convert_into_ms)

        #save the results
        self._write_csv(df, "qualifying_staging.csv")






       

