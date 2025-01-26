# Module 1 Homework: Docker & SQL

## Question 1. Understanding docker first run

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1
- 24.2.1
- 23.3.1
- 23.2.1

### Solution

```
$ docker run -it --entrypoint=bash python:3.12.8
$ root@73120aa93bae:/# pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: "postgres"
      POSTGRES_PASSWORD: "postgres"
      POSTGRES_DB: "ny_taxi"
    ports:
      - "5433:5432"
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

If there are more than one answers, select only one of them

### Solution

Within the Docker network created by docker-compose, containers talk to each other via their service name and the container’s internal port, not the host-mapped port. In this setup, the service name is db and the Postgres internal port is 5432.

pgadmin will connect to:

```
db:5432
```

## Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

### Solution

Set up Postgres and PgAdmin via Docker Compose, and then ingest data into Postgres using a Python script.

**Postgres and PgAdmin**

Use the `docker-compose.yaml` provided to spin up two services:

- **db (Postgres)**
- **pgadmin (PgAdmin 4)**

```bash
docker-compose up
```

This command will:

- Pull and run the postgres:17-alpine image, exposing Postgres on port 5433 of your host machine.
- Pull and run the dpage/pgadmin4:latest image, exposing PgAdmin on port 8080 of your host machine.
- Create volumes to persist database and PgAdmin data.

Inside the Docker network, PgAdmin connects to Postgres by using db:5432 (the service name and the container’s internal port).

**Data Ingestion**

I use a Python script, ingest_data.py, to download data (from CSV, CSV.GZ, or Parquet), optionally unzip or convert it, and then load it into Postgres.

**Loading Green Taxi Data**

The following command downloads the October 2019 Green Taxi data (in GZipped CSV format) and ingests it into the green_taxi_data table in the ny_taxi database:

```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

python scripts/ingest_data.py \
  --user=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=green_taxi_data \
  --datetime_cols="tpep_pickup_datetime,tpep_dropoff_datetime" \
  --url="${URL}"
```

params:

- --password (optional, default from env var)
- --datetime_cols tells the script which columns to convert to a date/time format.
  - --table_name is the target table in Postgres.
  - --url is the source dataset file.

**Loading Taxi Zone Lookup Data**

```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

python scripts/ingest_data.py \
  --user=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=zones \
  --url="${URL}"
```

This creates or appends to a table called zones in the same database.

**Resulting Tables**

After running both ingestion commands, you’ll have two tables in your Postgres database:

- green_taxi_data
- zones

**Supported File Formats**

The ingestion script supports:

- Parquet (.parquet)
- CSV (.csv)
- Compressed CSV (.csv.gz)

It automatically detects file extensions and downloads, unzips, or converts the file as needed. You can specify any columns you want to parse as datetime using `--datetime_cols`.

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:

1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles

Answers:

- 104,802; 197,670; 110,612; 27,831; 35,281
- 104,802; 198,924; 109,603; 27,678; 35,189
- 104,793; 201,407; 110,612; 27,831; 35,281
- 104,793; 202,661; 109,603; 27,678; 35,189
- 104,838; 199,013; 109,645; 27,688; 35,202

### Solution

```sql
SELECT
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS up_to_1_mile,
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS between_1_and_3_miles,
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS between_3_and_7_miles,
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS between_7_and_10_miles,
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS over_10_miles
FROM green_taxi_data
WHERE CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'
AND CAST(lpep_dropoff_datetime AS DATE) < '2019-11-01';
```

```
104,802;  198,924;  109,603;  27,678;  35,189
```

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31

### Solution

```sql
SELECT
  lpep_pickup_datetime,
  trip_distance
FROM green_taxi_data
ORDER BY trip_distance DESC
LIMIT 1;
```

```
2019-10-31
```



## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.

- East Harlem North, East Harlem South, Morningside Heights
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park

```bash
SELECT 
    z."Zone",
    COUNT(*) AS trip_count,
    SUM(g.total_amount) AS total_amount
FROM green_taxi_data AS g
JOIN zones AS z ON g."PULocationID" = z."LocationID"
WHERE CAST(g.lpep_pickup_datetime AS DATE) = '2019-10-18' 
  AND g.total_amount > 13
GROUP BY z."Zone"
ORDER BY total_amount DESC;
```

```
- East Harlem North, East Harlem South, Morningside Heights
```


## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
named "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport
- East Harlem North
- East Harlem South

### Solution

```sql
SELECT 
	pu_z."Zone" AS pickup_zone,
    pu_z."Borough" AS pickup_borough,
    do_z."Zone" AS dropoff_zone,
    do_z."Borough" AS dropoff_borough,
    g."PULocationID",
    g."DOLocationID",
    g.tip_amount
FROM green_taxi_data AS g
JOIN zones AS pu_z ON g."PULocationID" = pu_z."LocationID"
JOIN zones AS do_z ON g."DOLocationID" = do_z."LocationID"
WHERE CAST(g.lpep_dropoff_datetime AS DATE) >= '2019-10-01'
  AND CAST(g.lpep_dropoff_datetime AS DATE) < '2019-10-31'
  AND pu_z."Zone" = 'East Harlem North'
ORDER BY g.tip_amount DESC;
```

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform.
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for:

1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:

- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm

### Solution

```
terraform init, terraform apply -auto-approve, terraform destroy
```

Explanation:
1.	terraform init: Downloads provider plugins and sets up the backend.
2.	terraform apply -auto-approve: Automatically generates and applies the plan.
3.	terraform destroy: Removes all resources managed by Terraform../google-cloud-sdk/bin/gcloud init


Note:

Policy to be fixed:

* Service account key creation is disabled
* constraints/storage.uniformBucketLevelAccess