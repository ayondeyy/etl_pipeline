from utils import extraction, transformation, loading

print("\n----- Runing ETL Pipeline -----")

tables = extraction.extract()
data = transformation.transform(tables)
loading.load(data)