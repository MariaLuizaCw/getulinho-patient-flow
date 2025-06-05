from graphviz import Digraph, Source
import psycopg2
import os

# Function to fetch transitions from the database
def fetch_transitions():
    # conn = psycopg2.connect(
    #     dbname="hospital_db",
    #     user="user",
    #     password="password",
    #     host="localhost",
    #     port="5432"
    # )
    # cursor = conn.cursor()
    # cursor.execute(
    #     "SELECT source, target, time_minutes FROM transitions WHERE time_minutes BETWEEN %s AND %s;",
    #     (min_time, max_time)
    # )
    # transitions = cursor.fetchall()
    # cursor.close()
    # conn.close()
    
    transitions = [
        ("Admission", "Triage", 5),
        ("Triage", "Consultation", 15),
        ("Consultation", "Tests", 10),
        ("Tests", "Diagnosis", 20),
        ("Diagnosis", "Discharge", 10)
    ]
    return transitions

# Function to generate the flowchart
def generate_fluxogram(transitions):
    dot_file_path = os.path.join("/app/fluxogram.dot")
    with open(dot_file_path, 'r') as file:
        dot_content = file.read()
    output_path = os.path.join("fluxogram")
    # Create a Graphviz object
    graph = Source(dot_content)

    # Render the .png image
    output_file = graph.render(output_path, format='png', cleanup=True)
    
    return f"{output_path}.png"
