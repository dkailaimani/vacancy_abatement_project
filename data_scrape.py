import mysql.connector
from mysql.connector import Error
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from password import cloud_password  # Import your database password here

# Function to establish database connection
def get_db_connection():
    host = '148.72.118.86'  # Replace with your database host
    database = 'vacancy_abatement'  # Replace with your database name
    user = 'dprice'  # Replace with your database username
    password = cloud_password  # Replace with your database password

    try:
        # Connect to MySQL database with autocommit and reconnect options
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            autocommit=True,  # Ensure autocommit is enabled
            connection_timeout=300,  # Increase timeout to handle long queries
            pool_size=5,  # Adjust pool size as needed
            buffered=True  # Use buffered cursor
        )
        print("Connected to MySQL database successfully")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# Function to clean up monetary values
def clean_up_monetary_value(value):
    if pd.isna(value):  # Check if the value is NaN
        return None
    else:
        # Convert to string and remove dollar sign ($) and commas (,)
        cleaned_value = str(value).replace('$', '').replace(',', '')
        try:
            # Convert cleaned value to float (handles cases like '0.00' and '12,345.67')
            cleaned_value = int(cleaned_value)
        except ValueError:
            # If value cannot be converted to float, return None
            return None
        return cleaned_value

# Function to execute query with retry logic
def execute_query_with_retry(connection, query, params):
    retry_attempts = 3  # Number of retry attempts
    retry_delay = 5  # Delay in seconds between retries

    for attempt in range(retry_attempts):
        try:
            cursor = connection.cursor()
            cursor.execute(query, params)
            print("Query executed successfully")
            return True  # Return True on success

        except mysql.connector.Error as e:
            print(f"Attempt {attempt+1}: Error executing query: {e}")
            if attempt < retry_attempts - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                connection.reconnect()  # Reconnect to MySQL server
            else:
                print("Query execution failed after multiple attempts")
                return False  # Return False on failure

# Function to scrape data and insert/update database
def scrape_data(connection):
    if connection is None:
        print("Database connection is not available")
        return

    try:
        cursor = connection.cursor()

        # Fetch PIN numbers from properties table
        cursor.execute("SELECT PIN FROM properties;")
        pin_results = cursor.fetchall()

        # Extract PINs from result set 
        pin_numbers = [str(row[0]) for row in pin_results]
        

        # URL base for fetching property details
        base_url = 'https://www.cookcountyassessor.com/pin/'

        # List to store all property data dictionaries
        all_data = []

        # Iterate over each PIN number
        for pin in pin_numbers:
    # Pad PIN with zeros to ensure it is 14 digits long
            if len(pin) == 10:
                padded_pin = pin + '0000'
            else:
                padded_pin = pin

            url = f'{base_url}{padded_pin}#address'
            print(f"Fetching data for PIN: {padded_pin}")

            try:
                response = requests.get(url)

                # Check if the request was successful
                if response.status_code == 200:
                    html_text = response.text
                    soup = BeautifulSoup(html_text, 'html.parser')

                    # Dictionary to store property details
                    property_data = {}
                    property_data['Pin'] = pin  # Store PIN in data

                    # Mapping of web page labels to database columns
                    label_to_column = {
                        'Pin':'Pin',
                        'Address': 'Address',
                        'City': 'City',
                        'Township': 'Township',
                        'Property Classification': 'PropertyClassification',
                        'Square Footage (Land)': 'SquareFootage',
                        'Neighborhood': 'Neighborhood',
                        'Taxcode': 'Taxcode',
                        'Next Scheduled Reassessment': 'NextScheduledReassessment',
                        'Description': 'Description',
                        'Age': 'Age',
                        'Building Square Footage': 'BuildingSquareFootage',
                        'Assessment Phase': 'AssessmentPhase',
                        'Previous Board Certified': 'PreviousBoardCertified',
                        'Status': 'Status',
                        'Assessor Valuation': 'AssessorValuation',
                        'Assessor Post-Appeal Valuation': 'AssessorPostAppealValuation',
                        'Appeal Number': 'AppealNumber',
                        'Attorney/Tax Representative': 'AttorneyTaxRepresentative',
                        'Applicant': 'Applicant',
                        'Result': 'Result',
                        'Reason': 'Reason',
                        'Tax Year': 'TaxYear',
                        'Certificate Number': 'CertificateNumber',
                        'Property Location': 'PropertyLocation',
                        'C of E Description': 'COfEDescription',
                        'Comments': 'Comments',
                        'Residence Type': 'ResidenceType',
                        'Use': 'Use',
                        'Apartments': 'Apartments',
                        'Exterior Construction': 'ExteriorConstruction',
                        'Full Baths': 'FullBaths',
                        'Half Baths': 'HalfBaths',
                        'Basement1': 'Basement1',
                        'Attic': 'Attic',
                        'Central Air': 'CentralAir',
                        'Number of Fireplaces': 'NumberOfFireplaces',
                        'Garage Size/Type2': 'GarageSizeType2'
                    }

                    # Find all detail row containers
                    detail_rows = soup.find_all(['div', 'span'], class_=['detail-row', 'detail-row--label', 'col-xs-3 pt-header',
                                                                        'col-xs-2', 'detail-row--detail', 'large', 'col-xs-4', 'col-xs-5', 'small'])
                    column_name = None
                    # Iterate over each detail row and extract label and value
                    for row in detail_rows:
                        if 'detail-row--label' in row.get('class', []):
                            label = row.text.strip()
                            if label in label_to_column:
                                column_name = label_to_column[label]
                                property_data[column_name] = None  # Initialize with None
                        elif 'detail-row--detail' in row.get('class', []):
                            value = row.text.strip()
                            if column_name in property_data:
                                property_data[column_name] = value
                            else:
                                print("Skipping value because column_name is not defined")

                    # Append property data to all_data list
                    all_data.append(property_data)
                else:
                    print(f"Failed to retrieve page for PIN {pin}, status code: {response.status_code}")
                    print(response.text)  # Print response content for debugging

            except requests.RequestException as e:
                print(f"Request failed for PIN {pin}: {e}")

        # Convert all_data to pandas DataFrame
        df = pd.DataFrame(all_data)

        print(f"Number of columns in DataFrame: {len(df.columns)}")

        # Insert data into MySQL database
        try:
            # Prepare SQL query for insertion or update
            insert_query = f"""
                INSERT INTO property_details (
                    `Pin`, `Address`, `City`, `Township`, `PropertyClassification`, `SquareFootage`,
                    `Neighborhood`, `Taxcode`, `NextScheduledReassessment`, `Description`, `Age`,
                    `BuildingSquareFootage`, `AssessmentPhase`, `PreviousBoardCertified`, `Status`,
                    `AssessorValuation`, `AssessorPostAppealValuation`, `AppealNumber`,
                    `AttorneyTaxRepresentative`, `Applicant`, `Result`, `Reason`, `TaxYear`,
                    `CertificateNumber`, `PropertyLocation`, `COfEDescription`, `Comments`,
                    `ResidenceType`, `Use`, `Apartments`, `ExteriorConstruction`, `FullBaths`,
                    `HalfBaths`, `Basement1`, `Attic`, `CentralAir`, `NumberOfFireplaces`,
                    `GarageSizeType2`
                ) VALUES (
                    %(Pin)s, %(Address)s, %(City)s, %(Township)s, %(PropertyClassification)s, %(SquareFootage)s,
                    %(Neighborhood)s, %(Taxcode)s, %(NextScheduledReassessment)s, %(Description)s, %(Age)s,
                    %(BuildingSquareFootage)s, %(AssessmentPhase)s, %(PreviousBoardCertified)s, %(Status)s,
                    %(AssessorValuation)s, %(AssessorPostAppealValuation)s, %(AppealNumber)s,
                    %(AttorneyTaxRepresentative)s, %(Applicant)s, %(Result)s, %(Reason)s, %(TaxYear)s,
                    %(CertificateNumber)s, %(PropertyLocation)s, %(COfEDescription)s, %(Comments)s,
                    %(ResidenceType)s, %(Use)s, %(Apartments)s, %(ExteriorConstruction)s, %(FullBaths)s,
                    %(HalfBaths)s, %(Basement1)s, %(Attic)s, %(CentralAir)s, %(NumberOfFireplaces)s,
                    %(GarageSizeType2)s
                )
                ON DUPLICATE KEY UPDATE
                    `Address` = VALUES(`Address`),
                    `City` = VALUES(`City`),
                    `Township` = VALUES(`Township`),
                    `PropertyClassification` = VALUES(`PropertyClassification`),
                    `SquareFootage` = VALUES(`SquareFootage`),
                    `Neighborhood` = VALUES(`Neighborhood`),
                    `Taxcode` = VALUES(`Taxcode`),
                    `NextScheduledReassessment` = VALUES(`NextScheduledReassessment`),
                    `Description` = VALUES(`Description`),
                    `Age` = VALUES(`Age`),
                    `BuildingSquareFootage` = VALUES(`BuildingSquareFootage`),
                    `AssessmentPhase` = VALUES(`AssessmentPhase`),
                    `PreviousBoardCertified` = VALUES(`PreviousBoardCertified`),
                    `Status` = VALUES(`Status`),
                    `AssessorValuation` = VALUES(`AssessorValuation`),
                    `AssessorPostAppealValuation` = VALUES(`AssessorPostAppealValuation`),
                    `AppealNumber` = VALUES(`AppealNumber`),
                    `AttorneyTaxRepresentative` = VALUES(`AttorneyTaxRepresentative`),
                    `Applicant` = VALUES(`Applicant`),
                    `Result` = VALUES(`Result`),
                    `Reason` = VALUES(`Reason`),
                    `TaxYear` = VALUES(`TaxYear`),
                    `CertificateNumber` = VALUES(`CertificateNumber`),
                    `PropertyLocation` = VALUES(`PropertyLocation`),
                    `COfEDescription` = VALUES(`COfEDescription`),
                    `Comments` = VALUES(`Comments`),
                    `ResidenceType` = VALUES(`ResidenceType`),
                    `Use` = VALUES(`Use`),
                    `Apartments` = VALUES(`Apartments`),
                    `ExteriorConstruction` = VALUES(`ExteriorConstruction`),
                    `FullBaths` = VALUES(`FullBaths`),
                    `HalfBaths` = VALUES(`HalfBaths`),
                    `Basement1` = VALUES(`Basement1`),
                    `Attic` = VALUES(`Attic`),
                    `CentralAir` = VALUES(`CentralAir`),
                    `NumberOfFireplaces` = VALUES(`NumberOfFireplaces`),
                    `GarageSizeType2` = VALUES(`GarageSizeType2`)
            """

            # Iterate over rows in DataFrame and insert/update data
            for index, row in df.iterrows():
                # Clean up PIN and other columns as needed
                if pd.notna(row['Pin']):
                    if isinstance(row['Pin'], str):
                        row['Pin'] = int(row['Pin'].replace('-', ''))

                if pd.notna(row['SquareFootage']):
                    if isinstance(row['SquareFootage'], str):
                        row['SquareFootage'] = int(row['SquareFootage'].replace(',', ''))

                if pd.notna(row['BuildingSquareFootage']):
                    if isinstance(row['BuildingSquareFootage'], str):
                        try:
                            row['BuildingSquareFootage'] = int(row['BuildingSquareFootage'].replace(',', ''))
                        except ValueError:
                            row['BuildingSquareFootage'] = None  # Handle invalid values

                # Clean up monetary values if needed
                if pd.notna(row['AssessorValuation']):
                    if isinstance(row['AssessorValuation'], str):
                        row['AssessorValuation'] = clean_up_monetary_value(row['AssessorValuation'])

                if pd.notna(row['AssessorPostAppealValuation']):
                    if isinstance(row['AssessorPostAppealValuation'], str):
                        row['AssessorPostAppealValuation'] = clean_up_monetary_value(row['AssessorPostAppealValuation'])

                if pd.notna(row['PreviousBoardCertified']):
                    if isinstance(row['PreviousBoardCertified'], str):
                        row['PreviousBoardCertified'] = clean_up_monetary_value(row['PreviousBoardCertified'])

                # Convert Pandas Series to dictionary
                row_dict = row.to_dict()
                row_dict = {key: (None if pd.isna(value) else value) for key, value in row_dict.items()}
                # Execute insertion or update query with retry logic
                execute_query_with_retry(connection, insert_query, row_dict)

                print(f"Row inserted/updated successfully: {row_dict}")

            # Commit changes to database
            connection.commit()
            print("Data insertion/update completed successfully")

        except Error as e:
            print(f"Error inserting/updating data into MySQL database: {e}")

    finally:
        # Close cursor and connection
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection is not None:
            connection.close()
            print("MySQL connection is closed")

def main():
    connection = get_db_connection()
    if connection:
        scrape_data(connection)
if __name__ == "__main__":
    main()
