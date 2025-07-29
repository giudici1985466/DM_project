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
