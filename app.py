from flask import Flask, jsonify, request
from flask_marshmallow import Marshmallow
from marshmallow import fields, ValidationError
import mysql.connector
from mysql.connector import Error
from password import root_password

from flask_cors import CORS

app = Flask(__name__)
CORS(app)
ma = Marshmallow(app)

class properties_schema(ma.Schema):
    PropertyID = fields.Integer(required=True)
    StreetNumber = fields.String(required=True)
    PIN = fields.Integer(required=True)
    Owner = fields.String(required=True)
    Address = fields.String(required=True)
    City = fields.String(required=True)
    State = fields.String(required=True)
    Zipcode = fields.Integer(required=True)
    SquareFeet = fields.Integer(required=True)
    Link = fields.String(required=True)

    class Meta: 
        fields = ("PropertyID", "StreetNumber", "PIN", "Owner", "Address", "City", "State", "Zipcode", "SquareFeet", "Link")

properties = properties_schema()
properties_multi = properties_schema(many=True)

def get_db_connection():
    """Connect to the MySQL database and return the connection object"""
    db_name = "vacancy_abatement"
    user = "root"
    my_password = root_password
    host = "localhost"

    try:
        conn = mysql.connector.connect(
            database = db_name,
            user = user, 
            password=my_password,
            host=host,
            port = 3306
        )
        print("Connected to MySQL database successfully")
        return conn
    except Error as e:
        print(f"Error: {e}")
        return None

@app.route("/properties", methods=["GET"])
def get_properties():
    try: 
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        
        cursor = conn.cursor(dictionary=True)

        query = "SELECT * FROM properties"
        cursor.execute(query)
        properties_data = cursor.fetchall()
        
        return jsonify(properties_multi.dump(properties_data))
    except Exception as e:
       print(f"Error: {e}")
       return jsonify({"error": "Internal Server Error"}), 500    
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route("/properties/<int:PropertyID>", methods=["GET"])
def get_property_by_id(PropertyID):
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM properties WHERE PropertyID = %s", (PropertyID,))
        property_data = cursor.fetchone()
        
        if property_data is None:
            return jsonify({"error": "Property not found"}), 404
        
        return jsonify(properties.dump(property_data))
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route("/properties", methods=["POST"])
def add_property():
    try:
        existing_property_data = properties.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400

    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        new_property = (
            existing_property_data["PropertyID"],
            existing_property_data["StreetNumber"],
            existing_property_data["PIN"],
            existing_property_data["Owner"],
            existing_property_data["Address"],
            existing_property_data["City"],
            existing_property_data["State"],
            existing_property_data["Zipcode"],
            existing_property_data["SquareFeet"],
            existing_property_data["Link"]
        )
        query = "INSERT INTO properties (PropertyID, StreetNumber, PIN, Owner, Address, City, State, Zipcode, SquareFeet, Link) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(query, new_property)
        conn.commit()

        return jsonify({"message":"New property added successfully"}), 201
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally: 
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route("/properties/<int:PropertyID>", methods=["PUT"])
def update_property(PropertyID):
    try:
        existing_property_data = properties_schema().load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400

    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        updated_property = (
            existing_property_data["StreetNumber"],
            existing_property_data["PIN"],
            existing_property_data["Owner"],
            existing_property_data["Address"],
            existing_property_data["City"],
            existing_property_data["State"],
            existing_property_data["Zipcode"],
            existing_property_data["SquareFeet"],
            existing_property_data["Link"],
            PropertyID
        )

        query = 'UPDATE properties SET StreetNumber = %s, PIN = %s, Owner = %s, Address = %s, City = %s, State = %s, Zipcode = %s, SquareFeet = %s, Link = %s WHERE PropertyID = %s'
        
        cursor.execute(query, updated_property)
        conn.commit()

        return jsonify({"message":"Property Updated Successfully"}), 200
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error":"Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# @app.route("/properties", methods=["PUT"])
# def update_properties():
    try:
        properties_data = properties_schema(many=True).load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400

    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        for property_data in properties_data:
            updated_property = (
                property_data["StreetNumber"],
                property_data["PIN"],
                property_data["Owner"],
                property_data["Address"],
                property_data["City"],
                property_data["State"],
                property_data["Zipcode"],
                property_data["SquareFeet"],
                property_data["Link"],
                property_data["PropertyID"]
            )

            query = 'UPDATE properties SET StreetNumber = %s, PIN = %s, Owner = %s, Address = %s, City = %s, State = %s, Zipcode = %s, SquareFeet = %s, Link = %s WHERE PropertyID = %s'
            cursor.execute(query, updated_property)
        
        conn.commit()

        return jsonify({"message":"Properties Updated Successfully"}), 200
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error":"Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


@app.route("/properties/<int:PropertyID>", methods=["DELETE"])
def delete_property(PropertyID):
    print(f"Received DELETE request for PropertyID: {PropertyID}")
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        property_to_remove = (PropertyID,)

        cursor.execute("SELECT * FROM properties WHERE PropertyID = %s", property_to_remove)
        fetched_property = cursor.fetchone()
        if not fetched_property:
            return jsonify({"error": "Property not found!"}), 404

        query = "DELETE FROM properties WHERE PropertyID = %s"
        cursor.execute(query, property_to_remove)
        conn.commit()

        return jsonify({"message": "Property removed successfully"}), 201
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
        

if __name__ == '__main__':
    app.run(port=5000, debug=True)

