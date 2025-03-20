# IMDB Spark Project

## Dataset

This project uses the **IMDB dataset**.

- **Data**: You can download the required data from the following link: [IMDB dataset](https://datasets.imdbws.com/)
- **Description**: For detailed information about the dataset, visit: [IMDB dataset description](https://developer.imdb.com/non-commercial-datasets/)

## Setup & Running the Project
You have two options for setting up and running the project: **Manually installing dependencies** or **Using Docker**.
### Option 1: Manually Installing Dependencies
If you prefer not to use Docker, you can manually set up the environment and install the necessary dependencies.
Use the following command to install the required dependencies:
```bash
pip install -r requirements.txt
````

### Option 2: Using Docker
1. **Build Docker image**:
```bash
docker build -t my-spark img .
```
2. **Run the container**:
```bash
docker run my-spark-img
```


