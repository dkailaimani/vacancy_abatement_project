from flask import Flask, jsonify, request
from bs4 import BeautifulSoup
import time
import requests
import mysql.connector
from mysql.connector import Error
import pandas as pd
from password import cloud_password  # Import your database password here

# Initialize Flask app
app = Flask(__name__)

# MySQL database configuration
db_config = {
    'host': '148.72.118.86',  # Replace with your database host
    'database': 'vacancy_abatement',  # Replace with your database name
    'user': 'dprice',  # Replace with your database username
    'password': cloud_password,  # Replace with your database password
    'autocommit': True,
    'connection_timeout': 300,
    'pool_size': 5,
    'buffered': True
}

# Function to establish MySQL connection
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(**db_config)
        print("Connected to MySQL database successfully")
        return conn
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
            # Convert cleaned value to integer (handles cases like '0.00' and '12,345')
            cleaned_value = int(cleaned_value)
        except ValueError:
            # If value cannot be converted to integer, return None
            return None
        return cleaned_value

def clean_up_square_footage(value):
    if pd.notna(value):
        # Replace commas and asterisks and then convert to int
        cleaned_value = value.replace(',', '').replace('*', '')
        try:
            return int(cleaned_value)
        except ValueError:
            return None  # Or handle the error as per your application's logic
    return None  # Or handle the case where value is None or not applicable

def clean_property_data(property_data):
    # Print original property_data for debugging
    print("Original property_data:", property_data)

    # Clean up 'Pin' field
    if pd.notna(property_data.get('Pin')):
        if isinstance(property_data['Pin'], str):
            # Remove dashes and convert to integer
            cleaned_pin = property_data['Pin'].replace('-', '')
            print("Cleaned Pin before conversion:", cleaned_pin)
            try:
                property_data['Pin'] = int(cleaned_pin)
            except ValueError as e:
                print("Error converting Pin to integer:", e)
                property_data['Pin'] = None  # Handle error case if needed

    # Print cleaned property_data for debugging
    print("Cleaned property_data:", property_data)

    return property_data


# Function to scrape data for a given PIN number
def scrape_property_data(pin_number):
    base_url = 'https://www.cookcountyassessor.com/pin/'
    url = f'{base_url}{pin_number}#address'

    try:
        response = requests.get(url)
        if response.status_code == 200:
            html_text = response.text
            soup = BeautifulSoup(html_text, 'html.parser')

            # Mapping of web page labels to database columns
            label_to_column = {
                'Pin': 'Pin',
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

            # Dictionary to store property details
            property_data = {'Pin': pin_number}

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

            return property_data

        else:
            print(f"Failed to retrieve page for PIN {pin_number}, status code: {response.status_code}")
            return None

    except requests.RequestException as e:
        print(f"Request failed for PIN {pin_number}: {e}")
        return None

# API endpoint for scraping and inserting property details
@app.route('/scrape-property', methods=['POST'])
def scrape_and_insert_property():
    pin_number = request.json.get('pin_number')

    if not pin_number:
        return jsonify({'error': 'PIN number is required'}), 400

    connection = connect_to_mysql()
    if not connection:
        return jsonify({'error': 'Failed to connect to database'}), 500

    try:
        # Scrape property data
        property_data = scrape_property_data(pin_number)

        if not property_data:
            return jsonify({'error': f'Failed to scrape data for PIN {pin_number}'}), 500

        # Insert into MySQL database
        try:
            # Clean up property data (if needed)
            if pd.notna(property_data['SquareFootage']):
                property_data['SquareFootage'] = int(property_data['SquareFootage'].replace(',', ''))

            if property_data['BuildingSquareFootage'].replace(',', '').isdigit():
                property_data['BuildingSquareFootage'] = int(property_data['BuildingSquareFootage'].replace(',', ''))
            else:
                property_data['BuildingSquareFootage'] = None  # Or another default value

                assessor_valuation = property_data.get('AssessorValuation')
            if assessor_valuation is not None and pd.notna(assessor_valuation):
                property_data['AssessorValuation'] = clean_up_monetary_value(assessor_valuation)
            else:
                property_data['AssessorValuation'] = None

            # Handle 'AssessorPostAppealValuation'
            assessor_post_appeal_valuation = property_data.get('AssessorPostAppealValuation')
            if assessor_post_appeal_valuation is not None and pd.notna(assessor_post_appeal_valuation):
                property_data['AssessorPostAppealValuation'] = clean_up_monetary_value(assessor_post_appeal_valuation)
            else:
                property_data['AssessorPostAppealValuation'] = None

            # Handle 'PreviousBoardCertified'
            previous_board_certified = property_data.get('PreviousBoardCertified')
            if previous_board_certified is not None and pd.notna(previous_board_certified):
                property_data['PreviousBoardCertified'] = clean_up_monetary_value(previous_board_certified)
            else:
                property_data['PreviousBoardCertified'] = None

            # Handle 'BuildingSquareFootage'
            building_square_footage = property_data.get('BuildingSquareFootage')
            if building_square_footage is not None and pd.notna(building_square_footage):
                property_data['BuildingSquareFootage'] = clean_up_square_footage(building_square_footage)
            else:
                property_data['BuildingSquareFootage'] = None

            # Prepare SQL query for insertion or update
            property_data = clean_property_data(property_data)

            insert_query = """
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
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
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
            data = (
            property_data.get('Pin'),
            property_data.get('Address'),
            property_data.get('City'),
            property_data.get('Township'),
            property_data.get('PropertyClassification'),
            property_data.get('SquareFootage'),
            property_data.get('Neighborhood'),
            property_data.get('Taxcode'),
            property_data.get('NextScheduledReassessment'),
            property_data.get('Description'),
            property_data.get('Age'),
            property_data.get('BuildingSquareFootage'),
            property_data.get('AssessmentPhase'),
            property_data.get('PreviousBoardCertified'),
            property_data.get('Status'),
            property_data.get('AssessorValuation'),
            property_data.get('AssessorPostAppealValuation'),
            property_data.get('AppealNumber'),
            property_data.get('AttorneyTaxRepresentative'),
            property_data.get('Applicant'),
            property_data.get('Result'),
            property_data.get('Reason'),
            property_data.get('TaxYear'),
            property_data.get('CertificateNumber'),
            property_data.get('PropertyLocation'),
            property_data.get('COfEDescription'),
            property_data.get('Comments'),
            property_data.get('ResidenceType'),
            property_data.get('Use'),
            property_data.get('Apartments'),
            property_data.get('ExteriorConstruction'),
            property_data.get('FullBaths'),
            property_data.get('HalfBaths'),
            property_data.get('Basement1'),
            property_data.get('Attic'),
            property_data.get('CentralAir'),
            property_data.get('NumberOfFireplaces'),
            property_data.get('GarageSizeType2')
        )
            # Execute insertion or update query with retry logic
            
            execute_query_with_retry(connection, insert_query, data)

            return jsonify({'message': f'Property details for PIN {pin_number} inserted/updated successfully'}), 201

        except Error as e:
            return jsonify({'error': f'Error inserting/updating data into MySQL database: {e}'}), 500

    finally:
        # Close MySQL connection
        if connection:
            connection.close()
            print("MySQL connection is closed")

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

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
