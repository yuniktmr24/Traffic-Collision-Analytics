# Cache Directory

Here the dataframes per year have been cached using the Apache parquet storage format (using gZip compression)

This will ensure that we don't have to recompute these dataframes every time we run the driver program and instead, we can simply load the already persisted dataframes (in parquet)
format and use that for our computations.
