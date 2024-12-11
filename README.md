# Vancouver Building Permits and Property Tax Data Analysis

This project performs data analysis on Vancouver's building permits and property tax data to generate insights on how residential and non-residential property investments impact land value. The analysis explores patterns in building permits and property taxes to identify trends, correlations, and factors influencing property values in Vancouver.

The project follows a multi-step process involving data extraction, transformation, and analysis, with interactive visualizations for public access.

## Quickstart

### To view the data analysis results and interact with the visualizations directly, you can simply click the following link to launch the project on [MyBinder](https://mybinder.org/v2/gh/Builderbot2000/bc-land-value-analysis.git/dev?labpath=cmpt732_project_visualization.ipynb) and click "▶▶". This will allow you run the notebook and explore the interactive visualizations without any setup.

### A preview heatmap of the processed project data can be accessed through this link:
### https://cloud.dekart.xyz/reports/3853c282-ffa2-46aa-9d12-5653b9328b3c/source

### A video demo of our study can be viewed here:
### https://youtu.be/1TgVYSrJKYI

### A report of our study can be read here:
### 

## Project Overview

The project involves the following steps:

1. **Data Acquisition**: Downloaded Parquet and CSV datasets from [OpenData Vancouver](https://opendata.vancouver.ca) and uploaded them to AWS S3.
2. **ETL Process with AWS Glue**: Used PySpark to perform data extraction, transformation, and loading (ETL) in AWS Glue. The cleaned data was then loaded into AWS Redshift for further processing.
3. **Data Processing with SQL in Redshift**: Performed filtering, joins, and aggregations on the datasets using SQL queries in AWS Redshift.
4. **Unloading Processed Data**: Unloaded the processed dataset from Redshift back to AWS S3 for further analysis.
5. **Data Analysis and Visualization**: Read the processed dataset into AWS SageMaker, where Python was used to generate interactive visualizations.
6. **Public Access to Visualizations**: The final interactive visualizations were pushed to this repository and are accessible publicly via [MyBinder](https://mybinder.org/v2/gh/Builderbot2000/bc-land-value-analysis.git/dev?labpath=cmpt732_project_visualization.ipynb).
7. **Heatmap Construction**: A portion of the data generated in this project is uploaded to GCP Big Query and displayed as a heatmap through dekart.xyz.

## Project Structure

- `building-permits-etl/ (Deprecated)`: AWS Glue ETL scripts for extracting and transforming building permits data.
- `building-permits-csv-etl/`: AWS Glue ETL scripts for extracting and transforming building permits data in csv so that geospatial data can pass through the pipeline.
- `business-licenses-etl/`: AWS Glue ETL scripts for extracting and transforming business licenses data.
- `property-tax-etl/`: AWS Glue ETL scripts for extracting and transforming property tax data.
- `local-area-boundary/`: Contains boundary data files for local areas, used in geographic analysis.
- `cmpt732_project_analysis.ipynb`: Jupyter notebook for performing the main data analysis and generating insights.
- `cmpt732_project_visualization.ipynb`: Jupyter notebook for creating visualizations based on the analysis.
- `cmpt732_project_visualization_data.ipynb`: Additional notebook for storing miscellaneous data.

## Key Objectives

- **Trend Analysis**: Investigate trends in residential vs. non-residential building permits and their effects on land values.
- **Correlation Exploration**: Examine correlations between types of property investments and land value changes.
- **Impact Assessment**: Assess how different types of developments influence the Vancouver land market.

## Technologies Used

- **AWS Glue**: For ETL processes (data extraction, transformation, and loading).
- **AWS Redshift**: For data processing with SQL queries (filtering, joins, aggregation).
- **AWS S3**: For storing raw, processed, and intermediate data.
- **AWS SageMaker**: For reading in processed data and generating interactive visualizations.
- **Python (Pandas, NumPy, Matplotlib, Seaborn)**: For data analysis and visualization.
- **Jupyter Notebooks**: For interactive analysis and exploration.
- **GCP Big Query**: For serving as data backend of heatmap
- **Kepler.gl / Dekart**: For generating 3D heatmap

## Data Sources

The following datasets were used in this project:

1. **Property Tax Data**: The property tax data used in this analysis is sourced from the City of Vancouver's open data portal. It provides information about property tax assessments, which is crucial for understanding property value trends over time. You can explore the dataset here: [Property Tax Report Dataset](https://opendata.vancouver.ca/explore/dataset/property-tax-report).

2. **Building Permits Data**: The building permits data is also sourced from the City of Vancouver's open data portal. This dataset provides detailed records of building permits issued for construction projects in Vancouver. It is used to analyze trends in construction and development activities. You can explore the dataset here: [Issued Building Permits Dataset](https://opendata.vancouver.ca/explore/dataset/issued-building-permits).

3. **Local Area Boundary Data**: The local area boundary data is sourced from the City of Vancouver's open data portal. This dataset contains the boundaries of the city's 22 local planning areas, which are used for various city planning and analysis purposes. These boundaries remain constant over time and generally follow street centrelines. This dataset provides a spatial context for the analysis of trends in property taxes and building permits. You can explore the dataset here: [Local Area Boundary Dataset](https://opendata.vancouver.ca/explore/dataset/local-area-boundary).

### How to Set Up the Project on AWS

If you'd like to replicate the full project on AWS, you can set up the environment using the code in this repository. You can:

1. Set up the ETL process using AWS Glue (for transforming and loading the data).
2. Configure AWS Redshift for SQL-based data processing (filtering, joins, aggregation).
3. Load the processed data into AWS SageMaker to run the analysis and create visualizations.

Once the AWS environment is set up, you can execute the ETL jobs and data analysis from scratch or explore the existing notebooks to review the results.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
