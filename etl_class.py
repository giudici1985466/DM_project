import pandas as pd
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
