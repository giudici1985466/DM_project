from RDLTransformation import ETLTransformation

if __name__ == "__main__":
    import sys

    #path
    input_dir = "etl_transformation/input_files"
    output_dir = "etl_transformation/output_files"

    etl = ETLTransformation(input_dir, output_dir)

    #transformations
    etl.races_processing()
    etl.constructor_results_processing()
    etl.constructor_standings_processing()
    etl.drivers_processing()
    etl.race_results_preprocessing()
    etl.seasons_processing()
    etl.race_lineup_processing()
    etl.speed_processing()
    etl.stints_processing()
    etl.lap_times_processing()
    etl.pit_stops_processing()
    etl.qualifying_processing()
    etl.stints_processing()
    
   
    print("All the staging csv have been created")
