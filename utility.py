import pandas as pd

#utility function for converting a time string into ms
def convert_into_ms(time_str):
        if pd.isna(time_str):
            return pd.NA
        try:
            minutes, rest = time_str.split(":")
            seconds, milliseconds = rest.split(".")
            total_ms = int(minutes) * 60 * 1000 + int(seconds) * 1000 + int(milliseconds)
            return total_ms
        except:
            return pd.NA 
        

def split_and_clean(nationality_field):
    if pd.isna(nationality_field):
        return []
    field = str(nationality_field).lower()
    return [part.strip().lower() for part in str(field).replace("/", "-").split("-")]

