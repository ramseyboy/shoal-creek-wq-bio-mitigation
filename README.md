Project data and exploratory analysis for GEOL 2389 - Geology/Hydrology Research Internship

### Final Research Poster

![Poster](GEOL%202389%20Group%201%20Research%20Poster.001.jpeg)

[PDF Link](GEOL%202389%20Group%201%20Research%20Poster.pdf)

### Database Access and Configuration

Notebooks and qgis project are associated with a PostGIS SQL Database.

In the config package there are two DB configs

1. `PostgresReadOnlyConfig`
   - This is the main read_only user that has access to the `public` schema in the PostGIS DB
   - It is also the user that the QGIS project is assumed to be using
2. `PostgresReadWriteConfig`
   - This is the user for doing ETL in the `pipelines` package
   - This user has access to the `public` and the `staging` schemas

They can be used in python code and notebooks like `PostgresReadOnlyConfig.__str__()` to produce a valid postgres connection str that can be fed into a sqlalchemy engine. 

Both configs rely on the specified environment variables to be set in your shell or environment.

### QGIS Project

A QGIS project file is located in the `qgis` directory. When opened it will prompt you for the read_only username and password to the PostGIS database. 