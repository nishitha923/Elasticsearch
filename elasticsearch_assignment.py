
from elasticsearch import Elasticsearch, exceptions
import pandas as pd
import os

# Connect to Elasticsearch
es = Elasticsearch(
    ["http://localhost:9200"],
    basic_auth=('elastic', 'MyV=Y7s=+t8AxJMfp1Y='),
    verify_certs=False
)

def createCollection(p_collection_name):
    """Creates a new collection in Elasticsearch."""
    p_collection_name = p_collection_name.lower()  # Ensure the collection name is lowercase
    try:
        if not es.indices.exists(index=p_collection_name):
            es.indices.create(index=p_collection_name)
            print(f"Collection '{p_collection_name}' created.")
        else:
            print(f"Collection '{p_collection_name}' already exists.")
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except exceptions.ApiError as e:
        print(f"Error creating collection: {e}")

def indexData(p_collection_name, p_exclude_column):
    """Indexes data from CSV into the specified collection."""
    file_path = 'employee_sample_data.csv'

    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' does not exist.")
        return

    try:
        data = pd.read_csv(file_path, encoding='ISO-8859-1')

        print("Columns in the CSV:", data.columns.tolist())

        # Fill NaN values appropriately
        nan_fills = {
            "Business Unit": "Unknown",
            "Gender": "Unknown",
            "Exit Date": "Unknown",
            "Job Title": "Unknown"
        }
        data.fillna(nan_fills, inplace=True)
        data['Age'] = data['Age'].fillna(0)  # Fill NaN for Age with 0

        # Clean the Annual Salary and Bonus % fields
        data['Annual Salary'] = data['Annual Salary'].replace({'\\$': '', ',': ''}, regex=True)  # Escaped backslash
        data['Bonus %'] = data['Bonus %'].replace({'%': ''}, regex=True)

        # Convert to float, handling errors explicitly
        data['Annual Salary'] = pd.to_numeric(data['Annual Salary'], errors='coerce').fillna(0)
        data['Bonus %'] = pd.to_numeric(data['Bonus %'], errors='coerce').fillna(0)

        # Exclude specified column if it exists
        if p_exclude_column in data.columns:
            data = data.drop(columns=[p_exclude_column])
        
        for index, document in data.iterrows():
            doc_dict = document.to_dict()
            # Ensure no NaN values are present
            doc_dict = {k: (v if pd.notna(v) else "Unknown") for k, v in doc_dict.items()}
            es.index(index=p_collection_name, id=doc_dict['Employee_ID'], body=doc_dict)
        
        print(f"Data indexed into collection '{p_collection_name}', excluding column '{p_exclude_column}'.")
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except exceptions.ApiError as e:
        print(f"Error indexing data: {e}")
    except KeyError as e:
        print(f"Key error: {e}. Please ensure 'Employee_ID' column exists in the data.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    """Searches for a specific value in a specified column."""
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    try:
        response = es.search(index=p_collection_name, body=query)
        return response['hits']['hits']
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except exceptions.ApiError as e:
        print(f"Error searching data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def getEmpCount(p_collection_name):
    """Gets the count of employees in the collection."""
    try:
        count = es.count(index=p_collection_name)
        return count['count']
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except exceptions.ApiError as e:
        print(f"Error getting employee count: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def delEmpById(p_collection_name, p_employee_id):
    """Deletes an employee by ID from the collection."""
    try:
        response = es.delete(index=p_collection_name, id=p_employee_id)
        if response['result'] == 'deleted':
            print(f"Employee '{p_employee_id}' deleted from collection '{p_collection_name}'.")
        else:
            print(f"Failed to delete employee '{p_employee_id}'.")
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except exceptions.ApiError as e:
        print(f"Error deleting employee: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def getDepFacet(p_collection_name):
    """Retrieves department facets from the collection."""
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    try:
        response = es.search(index=p_collection_name, body=query)
        return response['aggregations']['departments']['buckets']
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except exceptions.ApiError as e:
        print(f"Error getting department facets: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Example usage:
if __name__ == "__main__":
    v_nameCollection = 'hash_john_doe'  # Replace with your actual name
    v_phoneCollection = 'hash_1234'    # Replace with your actual last four digits

    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)
    print("Employee Count:", getEmpCount(v_nameCollection))
    indexData(v_nameCollection, 'Department')
    indexData(v_phoneCollection, 'Gender')
    delEmpById(v_nameCollection, 'E02003')
    print("Employee Count:", getEmpCount(v_nameCollection))
    print(searchByColumn(v_nameCollection, 'Department', 'IT'))
    print(searchByColumn(v_nameCollection, 'Gender', 'Male'))
    print(searchByColumn(v_phoneCollection, 'Department', 'IT'))
    print(getDepFacet(v_nameCollection))
    print(getDepFacet(v_phoneCollection))


