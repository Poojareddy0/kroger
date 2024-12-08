import os

# Define the path
path = os.path.join("Cloud Final Project", "dataset")

# Create the directories
os.makedirs(path, exist_ok=True)  # 'exist_ok=True' prevents an error if the directory already exists
