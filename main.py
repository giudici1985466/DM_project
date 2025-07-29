from RDLTransformation import ETLTransformation

if __name__ == "__main__":
    import sys

    #path
    input_dir = "etl_transformation/input_files"
    output_dir = "etl_transformation/output_files"

    etl = ETLTransformation(input_dir, output_dir)

    #perform the transformation
    etl.constructor_results_processing()
    etl.constructor_standings_processing()
    print("ETL completato con successo.")
