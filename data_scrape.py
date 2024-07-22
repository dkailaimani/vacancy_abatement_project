import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from mysql.connector import Error

from password import cloud_password


# Function to establish MySQL database connection
def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host='148.72.118.86',
            database='vacancy_abatement',
            user='dprice',
            password=cloud_password
        )
        print("Connected to MySQL database successfully")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None


# Function to scrape data and insert into MySQL database
def scrape_data(connection):
    if connection is None:
        print("Database connection is not available")
        return

    try:
        cursor = connection.cursor()

        # Fetch PIN numbers from properties table
        cursor.execute("SELECT PIN FROM properties;")
        pin_results = cursor.fetchall()

        # Extract PINs from result set and append four zeros
        pin_numbers = [int(str(row[0]) + '0000') for row in pin_results]

        # URL base for fetching property details
        base_url = 'https://www.cookcountyassessor.com/pin/'

        # List to store all property data dictionaries
        all_data = []

        # Iterate over each PIN number
        for pin in pin_numbers:
            url = f'{base_url}{pin}#address'
            print(f"Fetching data for PIN: {pin}")

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
                        'Square Footage': 'SquareFootage',
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
                        'Assessor Post Appeal Valuation': 'AssessorPostAppealValuation',
                        'Appeal Number': 'AppealNumber',
                        'Attorney Tax Representative': 'AttorneyTaxRepresentative',
                        'Applicant': 'Applicant',
                        'Result': 'Result',
                        'Reason': 'Reason',
                        'Tax Year': 'TaxYear',
                        'Certificate Number': 'CertificateNumber',
                        'Property Location': 'PropertyLocation',
                        'C Of E Description': 'COfEDescription',
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
                        'Number Of Fireplaces': 'NumberOfFireplaces',
                        'Garage Size Type2': 'GarageSizeType2'
                    }

                    # Find all detail row containers
                    detail_rows = soup.find_all(['div', 'span'], class_=['detail-row', 'detail-row--label', 'col-xs-3 pt-header',
                                                                        'col-xs-2', 'detail-row--detail', 'large', 'col-xs-4', 'col-xs-5', 'small'])

                    # Iterate over each detail row and extract label and value
                    for row in detail_rows:
                        if 'detail-row--label' in row.get('class', []):
                            label = row.text.strip()
                            if label in label_to_column:
                                column_name = label_to_column[label]
                                property_data[column_name] = None  # Initialize with None
                        elif 'detail-row--detail' in row.get('class', []):
                            value = row.text.strip()
                            if 'column_name' in locals():  # Check if column_name is defined
                                property_data[column_name] = value

                    # Append property data to all_data list
                    all_data.append(property_data)

                else:
                    print(f"Failed to retrieve page for PIN {pin}, status code: {response.status_code}")
                    print(response.text)  # Print response content for debugging

            except requests.RequestException as e:
                print(f"Request failed for PIN {pin}: {e}")

        # Convert all_data to pandas DataFrame
        df = pd.DataFrame(all_data)

        # Insert data into MySQL database
        try:
            # Prepare SQL query for insertion
            insert_query = f"""
                INSERT INTO property_details (
                    Pin, Address, City, Township, PropertyClassification, Neighborhood, Taxcode, NextScheduledReassessment, 
                    Description, Age, BuildingSquareFootage, AssessmentPhase, PreviousBoardCertified, Status, AssessorValuation, 
                    AssessorPostAppealValuation, AppealNumber, AttorneyTaxRepresentative, Applicant, Result, Reason, TaxYear, 
                    CertificateNumber, PropertyLocation, COfEDescription, Comments, ResidenceType, Use, Apartments, 
                    ExteriorConstruction, FullBaths, HalfBaths, Basement1, Attic, CentralAir, NumberOfFireplaces, GarageSizeType2
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            # Establish connection again for insertion
            conn = get_db_connection()
            if conn is None:
                print("Database connection failed")
                return

            cursor = conn.cursor()

            # Iterate over rows in DataFrame and insert data
            for index, row in df.iterrows():
                row_data = [row[attr] for attr in df.columns]

                # Execute insertion query
                cursor.execute(insert_query, tuple(row_data))
                print(f"Row inserted successfully: {row_data}")

            # Commit changes to database
            conn.commit()
            print("Data inserted into MySQL database successfully")

        except Error as e:
            print(f"Error inserting data into MySQL database: {e}")

        finally:
            # Close cursor and connection
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
                print("MySQL connection closed")

    except mysql.connector.Error as error:
        print(f"Error connecting to MySQL database: {error}")

    finally:
        # Close cursor and connection
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("MySQL connection closed")

# Main function to execute data scraping and insertion
if __name__ == "__main__":
    # Establish database connection
    connection = get_db_connection()
    if connection:
        # Call function to scrape data and insert into database
        scrape_data(connection)
