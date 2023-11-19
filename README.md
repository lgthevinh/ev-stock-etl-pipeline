  # EV Companies stock (ETL Pipeline)

  ## Overview
  The `EV Stock Data pipeline` is a data engineering project that extracts stock market data from an API, transforms it into a suitable format, and loads it into a database. This pipeline automates the process of retrieving and storing stock data, making it easier to analyze and make informed investment decisions.

  ## Functionality
  The `etl_pipeline` function is the core component of this project. It performs the following steps:

  1. **Extraction**: Connects to the stock market API and retrieves the latest stock data.
  2. **Transformation**: Cleans and preprocesses the data, removing any invalid or missing values.
  3. **Loading**: Transforms the data into a structured format, such as a pandas DataFrame.
  4. **Database Connection**: Connects to the database and creates a table to store the stock data.
  5. **Data Loading**: Loads the transformed data into the database table.
  6. **Completion Message**: Returns a success message if the ETL process completes successfully.

  ## Usage

  ### For local running and testing
  To use the `EV Stock Data pipeline` project, follow these steps:

  1. Clone the repository: `git clone https://github.com/your-username/ev-stock-etl-pipeline.git`
  2. Install the required dependencies: `pip install -r requirements.txt`
  3. Configure the API credentials and database connection settings in the `config.py` file.
  4. Run the `etl_pipeline` function to start the ETL process.

  ### For docker and airflow deploy

  (documenting...)

  ## Dependencies
  The following dependencies are required to run the `EV Stock Data pipeline` project:

  - Python 3.9.18
  - pandas
  - requests
  - mysql-connector-python (for MySQL database)

  ## Contributing
  Contributions to the `EV Stock Data pipeline` project are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request.

  ## License
  This project is licensed under the [MIT License](LICENSE).
