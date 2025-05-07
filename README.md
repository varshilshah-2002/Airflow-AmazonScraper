# 🌀 Airflow-AmazonScraper

An ETL pipeline to scrape, clean, and store Amazon product listings using **BeautifulSoup**, **PostgreSQL**, and **Apache Airflow** — all orchestrated and containerized using **Docker Compose**.

---

## 📁 Project Structure

Airflow-AmazonScraper/
├── dags/ # Airflow DAGs (data pipeline logic lives here)
├── logs/ # Airflow logs (generated at runtime)
├── .env # Environment variables (Postgres credentials, etc.)
├── .DS_Store # (Ignore this - MacOS metadata file)
├── Dockerfile # Airflow environment setup
├── docker-compose.yaml # Multi-container Docker setup for Airflow, Postgres, etc.
├── requirements.txt # Python dependencies (Airflow, BeautifulSoup, etc.)


---

## ⚙️ Requirements

- Docker
- Docker Compose
- 4GB+ RAM recommended for running Airflow containers smoothly

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/Airflow-AmazonScraper.git
cd Airflow-AmazonScraper

2. Set Environment Variables
Edit the .env file to customize Postgres credentials if needed:
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

3. Build & Start Services
docker-compose up --build

This will:

Build the Airflow and Postgres containers
Initialize Airflow metadata DB
Start the Airflow scheduler, webserver, and Postgres service
4. Access Airflow UI
Visit: http://localhost:8080

Username: airflow
Password: airflow

📊 Pipeline Overview

The ETL DAG does the following:

Extract: Scrapes Amazon product data using BeautifulSoup.
Transform: Cleans and structures scraped HTML data.
Load: Inserts the cleaned data into a PostgreSQL table.
Schedule: Runs daily via Airflow's DAG scheduler.
🛠 Customization

Add or modify scraping logic in the dags/ folder.
Update requirements.txt for additional Python dependencies.
Use docker-compose down -v to reset the environment.
🧪 Run DAG Manually (Optional)

Once inside the Airflow container:
docker exec -it airflow-webserver bash
airflow dags list
airflow dags trigger your_dag_id

🧼 Stopping & Cleanup
docker-compose down

To remove volumes (including Postgres data):
docker-compose down -v

📌 To-Do / Future Improvements

Add more robust error handling and retries
Implement email alerts for DAG failures
Optionally store results in cloud (S3, GCS)


📄 License

This project is licensed under the MIT License.

Let me know if you want this tailored for uploading to GitHub with badges or deployment instructions!
