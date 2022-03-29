# mineral-evolution-data

## Installation
To run this code on your device, use `git clone` to download the code and within the downloaded directory run
```
conda create --name venv --file requirements.txt
```
This will create a virtual Python environment with all the required packages. You can then use `conda activate venv` to
activate the virtual environment and run the scripts in this repository. To open and run the .ipynb notebooks in this
repository, use `jupyter notebook`.

## MongoDB Database Restoration
Assuming you already have the MongoDB CLI utilities installed and configured, to restore the database containing all
the data used in this project, use `mongorestore dump/` within the cloned repository.
