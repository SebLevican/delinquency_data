### Scoring Script for Database Analysis

This Python script `scoring_script.py` is designed to fetch data from a MySQL database, process it, and calculate a scoring metric based on recent data. Here's a breakdown of its functionality:

1. **Setup Database Connection**:
   - The script uses `pymysql` and `sqlalchemy` to connect to a MySQL database. You need to provide your database host, username, password, and database name for the connection.

2. **Retrieve Recent Data**:
   - It fetches data from a specified table (`tabla_datos`) where the date is within the last year from the current date. This is achieved using SQL queries executed through `pandas` and `sqlalchemy`.

3. **Data Processing and Scoring**:
   - Once the data is fetched into a pandas DataFrame (`df`), you can implement your custom logic to process the data and calculate a scoring metric. This part would typically involve data manipulation, calculations, and applying business rules specific to your scoring requirements.

4. **Execution Control with `if __name__ == "__main__":`**:
   - This block ensures that the main logic (`main()` function) executes only when you run this script directly (`python scoring_script.py`). It prevents the code from running if the script is imported as a module into another script.

5. **Customization**:
   - You can extend the script by adding more data processing steps, integrating machine learning models for scoring, or exporting results to other formats or databases as needed.

### Usage
To use this script:

1. Ensure you have Python installed along with required libraries (`pymysql`, `sqlalchemy`, `pandas`).
2. Update the database connection details (`host`, `user`, `password`, `database`) in the script.
3. Run the script using `python scoring_script.py` in your terminal or command prompt.

### Dependencies
- Python 3.x
- `pymysql`
- `sqlalchemy`
- `pandas`

### Example
Here's a simple example of how to structure your scoring calculations using the script:

```python
# Example code snippet within main() function
scoring = GetScoring()
data = scoring.getbase()

# Perform additional data processing and scoring logic here
# e.g., calculate scoring metrics based on business rules
