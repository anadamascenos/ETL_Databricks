# **ETL Implementation in Databricks**

## **Overview**

This repository demonstrates the implementation of an ETL (Extract, Transform, Load) process using the Databricks platform. Databricks was chosen for its ability to handle large-scale data efficiently and its collaborative features that integrate data analysis and machine learning tools into a single environment.

---

## **Why Use Databricks for ETL?**

### **Scalability and Performance**
- Process large-scale data using distributed clusters, optimizing the execution time of complex tasks.

### **Integration with Multiple Data Sources**
- Supports connectors for:
  - Relational databases (SQL Server, PostgreSQL, etc.)
  - Data lakes (Azure Data Lake, AWS S3)
  - APIs and local files (CSV, JSON, Parquet)

### **Collaborative and Reproducible Environment**
- Interactive notebooks enable:
  - Real-time collaboration
  - Code versioning
  - Sharing analyses seamlessly

### **Compatibility with Popular Languages**
- Supports PySpark, SQL, Scala, and R, providing flexibility in data transformation.

### **Cost Efficiency**
- Clusters can be configured to shut down automatically after processing, reducing unnecessary costs.

---

## **Key Components**

### **Cluster Creation**
The first step in Databricks involves creating a cluster, which is a group of virtual machines that work together to execute processing tasks. Clusters are essential for providing the computational resources necessary to handle large data volumes and perform complex operations.

#### **Cluster Capabilities**
- **Large-Scale Data Processing**: Leverages Spark's distributed architecture for fast and efficient data handling.
- **Execution of Code and Tasks**: Runs scripts in Python (PySpark), SQL, R, or Scala within the cluster.
- **Temporary Data Storage**: Stores intermediate data in memory or disk during processing.
- **Scalability**: Can be adjusted to handle varying workloads by adding nodes or increasing capacity.

![1](https://github.com/user-attachments/assets/7b1b2fe6-3410-4bf3-8002-aa0549091742)

---

## **Exporting a Table as CSV**

![2](https://github.com/user-attachments/assets/36da6389-fdbc-4d6f-b081-6882e7000fe3)


![3](https://github.com/user-attachments/assets/37df0d06-9898-40a7-adae-36c90e21dfb2)


To export a table as a CSV file in Databricks, follow these steps:
1. Use the `write.csv()` method in PySpark or SQL to specify the output format.
2. Define the target directory for the CSV file in your data lake or storage location.
3. Configure additional parameters, such as delimiter, headers, and partitioning.


---

# **dbutils Library in Databricks**

## **Overview**

The `dbutils` library is a native utility in Databricks designed to simplify common operations in notebook environments. It is widely used for file system management within the Databricks File System (DBFS), executing commands, handling data, and configuring parameters.

---

## **Key Commands and Use Cases**

### **1. Listing Directories and Files**

![4](https://github.com/user-attachments/assets/a9e3a328-8244-475d-9b11-571877cd5cde)


#### **Objective**
The command `dbutils.fs.ls(path)` lists the contents of a directory in DBFS. The `display()` method is used to visualize the results in a tabular format within the notebook.

#### **Why Use It?**
To check which directories and files are available at a specified path, such as the root (`/`) or `/FileStore/tables`.

---

### **2. Viewing File Contents**

![5](https://github.com/user-attachments/assets/54896b6f-a737-4b14-83be-60b639d94cd1)


#### **Objective**
The command `dbutils.fs.head(file_path)` displays the first few lines of a file in DBFS.

#### **Why Use It?**
This helps inspect the file's content to ensure the data is in the expected format (e.g., CSV, JSON) before processing.

---

### **3. Creating a List of Files for Deletion**

![6](https://github.com/user-attachments/assets/6c3cac71-573e-4122-85c1-5927e6612393)


#### **Objective**
Prepare a list of file paths to be deleted in DBFS.

#### **Why Use It?**
Centralizing files slated for deletion simplifies bulk operations and enhances management efficiency.

---

### **4. Iterating and Removing Files**

#### **Objective**
The `dbutils.fs.rm(path)` command removes a file or directory in DBFS. Using a `for` loop, each item in the list is iterated and deleted.

#### **Why Use It?**
To clean unnecessary files from the system, free up space, and maintain directory organization.

---

### **5. Verifying Directories After Changes**

![7](https://github.com/user-attachments/assets/e27bdec3-c47a-4da1-a22e-ea88a50357d3)

#### **Objective**
Re-list the contents of a directory (e.g., `/FileStore/tables`) to confirm the files have been removed.

#### **Why Use It?**
Ensures that the intended deletions were successfully applied, verifying the directory is updated as expected.

---

## **Creating Tables Using SQL in Databricks**

This procedure demonstrates how to use SQL commands to create databases and tables in Databricks, loading data directly from a CSV file stored in DBFS. This approach is effective for structuring data in a tabular format for future querying.

---

### **Steps**

#### **1. Creating a Database**

![8](https://github.com/user-attachments/assets/7175c45b-dc11-4cba-be7e-50bb030ed4fc)


- **Command**: `CREATE DATABASE`
- **Objective**: Create a database named `Teste` if it does not already exist.
- **Why Use It?** 
  To ensure the `Teste` database is available for storing tables. Using `IF NOT EXISTS` prevents errors if the database already exists.

#### **2. Setting the Database Scope**

- **Command**: `USE`
- **Objective**: Set the `Teste` database as the active scope for subsequent operations like table creation or queries.
- **Why Use It?**
  Directs all operations to the correct database, avoiding confusion with other databases.

---

## **3. Creating the “DRE” Table**

![DB](https://github.com/user-attachments/assets/d7dc1734-0855-4f02-a155-535c6e0ba210)


### **Command:** `CREATE TABLE`

- **Objective:**  
  Create a table called `DRE` using the CSV file located in the DBFS.

- **Specified Options:**  
  - **`USING csv`**: Specifies the file format as CSV.  
  - **`OPTIONS`**:  
    - `'path': '/FileStore/tables/DRE.csv'`: Sets the file path in the DBFS.  
    - `'header': 'true'`: Indicates that the first row of the file contains column names.  
    - `'inferSchema': 'true'`: Allows the system to deduce data types automatically for each column in the CSV file.

- **Why Execute?**  
  Transform the data from the CSV file into a SQL table within the `Teste` database, facilitating structured queries and analyses.

---

## **4. Querying Table Data**

![Sem título](https://github.com/user-attachments/assets/dcd494b2-88f9-40c3-9133-41a5ba3dc634)


### **Organizing Data Architecture**

### **Bronze Layer - Raw Data Processing**

The Bronze Layer is responsible for storing data in its raw form, before any processing or transformation. This layer ensures the integrity of source data, allowing for future auditing or reprocessing if necessary. It serves as the first entry point for data in the processing pipeline, maintaining a faithful and unaltered history of the information.

#### **Directory Structure**

/mnt/bronze  
&nbsp;&nbsp;&nbsp;&nbsp;├── dre/  
&nbsp;&nbsp;&nbsp;&nbsp;├── brands/  
&nbsp;&nbsp;&nbsp;&nbsp;├── categories/  
&nbsp;&nbsp;&nbsp;&nbsp;├── order_items/  
&nbsp;&nbsp;&nbsp;&nbsp;├── orders/  
&nbsp;&nbsp;&nbsp;&nbsp;├── staffs/  
&nbsp;&nbsp;&nbsp;&nbsp;├── stocks/  
&nbsp;&nbsp;&nbsp;&nbsp;└── stores/


Each directory contains data in Delta format, which is an optimized format for large volumes of data and facilitates read, write, and update operations.

## Data Sources

The raw data is extracted from various sources, such as CSV and JSON files, located in different paths in the system. Below are the details of the tables and their respective paths:

### CSV Tables:

- brands - Location: /FileStore/tables/brands.csv
- categories - Location: /FileStore/tables/categories.csv
- order_items - Location: /FileStore/tables/order_items.csv
- orders - Location: /FileStore/tables/orders.csv
- staffs - Location: /FileStore/tables/staffs.csv
- stocks - Location: /FileStore/tables/stocks.csv
- stores - Location: /FileStore/tables/stores.csv
- dre - Location: /FileStore/tables/dre.csv

## Implemented Code

The code responsible for this processing is as follows:

![3](https://github.com/user-attachments/assets/9ddc02c2-20ae-4eab-bc20-581dba7b800a)


# Bronze Layer Benefits

- **Data Integrity**: Data is stored in its raw, immutable form, allowing for future reprocessing with the original data.
- **Auditing**: The Bronze layer provides an audit point to ensure traceability of data throughout the pipeline.
- **Scalability**: Using the Delta format, large-scale read and write operations can be performed efficiently.
- **Ease of Future Processing**: Data in the Bronze layer is ready for further processing in subsequent layers (Silver and Gold), with the possibility of applying transformations or additional enrichments.

---

# Silver Layer - Refining the Data

The **Silver layer** was created to provide more refined and structured data from the Bronze layer. Tables in this layer are cleaner and consist of information that is ready for analysis, with basic transformations applied, such as removing duplicates and converting data types.

## **Silver Layer Structure**

### **1. Processed Tables**

The **Silver layer** contains the following tables:

| **Table**       | **Description**                                             | **File Location**          |
|-----------------|-------------------------------------------------------------|----------------------------|
| **brands**      | Information about available brands                          | `/mnt/silver/brands`       |
| **categories**  | Categories of products or services offered                  | `/mnt/silver/categories`   |
| **order_items** | Items of each order placed on the platform                  | `/mnt/silver/order_items`  |
| **orders**      | Details of orders placed by customers                       | `/mnt/silver/orders`       |
| **staffs**      | Information about the staff involved in the orders          | `/mnt/silver/staffs`       |
| **stocks**      | Available stock quantity for products                       | `/mnt/silver/stocks`       |
| **stores**      | Details about stores                                        | `/mnt/silver/stores`       |
| **dre**         | Revenue and accounting data                                 | `/mnt/silver/dre`          |

---

## **Processing the Tables**

The tables are processed from the **Bronze layer** to the **Silver layer** following these steps:

1. **Data Reading**: Tables are read from the Bronze layer, which contains raw, unprocessed data.
2. **Removing Duplicates**: Duplicate entries are removed to ensure data integrity.
3. **Data Type Conversion**: Data types are corrected where necessary (e.g., converting string to date).
4. **Saving in Silver Layer**: After the transformations, the data is saved in the Silver layer.

# Storage in the Silver Layer

Processed tables are saved in the Silver layer in Delta format, ensuring they can be easily accessed and are ready for advanced analysis or aggregations.

## Save Command:

![4](https://github.com/user-attachments/assets/59a2874c-0853-4672-9e2f-fa787029af21)

### Table Creation in the Catalog:
The tables are also registered in the Delta catalog for easy access through SQL queries.

![1](https://github.com/user-attachments/assets/2654001b-8f4e-4b57-ac99-b240f76b4868)

# Benefits of the Silver Layer

- **Data Cleansing and Consistency**: The Silver layer ensures that the data is consistent and clean, with duplicates removed and data types corrected.
- **Readiness for Analysis**: Tables in the Silver layer are ready for advanced analysis, aggregations, and dashboards.
- **Storage in Delta**: The use of Delta format enables ACID transactions, ensuring data integrity and allowing versioning and change control.

## How to Use Tables in the Silver Layer

To access tables in the Silver layer for queries, you can use either SQL or PySpark:

### SQL:

![2](https://github.com/user-attachments/assets/b56e5c11-be17-4a61-92ae-e0c4618b758a)

### Python: 

![3](https://github.com/user-attachments/assets/84b5fb71-b688-42ef-8c8b-2ee7cbb55361)

---

# Gold Layer - Structuring for Analysis

The Gold layer represents the final stage in the data pipeline, where data is fully prepared for analysis and reporting. In this layer, data is aggregated, consolidated, and structured to directly meet business needs and provide strategic insights.

## Structure of the Gold Layer

The Gold layer contains tables that provide refined analytical information. Currently, the following tables are available:

| Table                | Description                                              | File Location               |
|----------------------|----------------------------------------------------------|-----------------------------|
| gold_clients         | Consolidated information about clients, including number of orders and total amount spent | /mnt/gold/gold_clients       |
| gold_store_performance | Store performance metrics, such as total revenue, number of orders, and remaining stock | /mnt/gold/gold_store_performance |

## Processing the Tables

### 1. gold_clients Table

**Description:**

This table consolidates information related to customers, based on orders and order items. It provides metrics such as:

- **Total Orders (total_orders):** Total number of orders placed by the customer.
- **Total Quantity (total_quantity):** Total quantity of items purchased.
- **Total Spent (total_spent):** Total value spent by the customer on items, including any discounts.
- **Total Discount (total_discount):** Total amount saved by the customer through discounts.

**Processing:**

- **Source Data:** Join between the `orders` and `order_items` tables from the Silver layer.
- **Transformations Applied:**
  - Grouping by `customer_id`.
  - Calculation of aggregated metrics.
- **Storage:** Data saved as a Delta table and registered in the Delta catalog under the `gold` schema.

**Example SQL Query:**

![4](https://github.com/user-attachments/assets/401ce67d-a616-44b7-932b-bdb1ca13318d)

## gold_store_performance Table

### Description:

This table presents performance metrics by store, considering sales, revenue, items sold, and remaining stock.

### Calculated Metrics:

- **Total Orders (total_orders)**: Total number of orders placed in each store.
- **Total Revenue (total_revenue)**: Gross revenue generated by each store, calculated as `(list_price * quantity) - discount`.
- **Total Items Sold (total_items_sold)**: Total quantity of items sold by each store.
- **Remaining Stock (remaining_stock)**: Sum of items still available in stock for each store.

### Processing:

- **Data Source**: Join of the `orders`, `order_items`, `stores`, and `stocks` tables.
  
- **Transformations Applied**:
  - Renaming the `quantity` column in the `stocks` table to `stock_quantity` to avoid name conflicts.
  - Grouping by `store_id` and `store_name`.
  - Calculating the aggregated metrics listed above.

- **Storage**: Data is saved as a Delta table and registered in the Delta catalog under the `gold` schema.

### Example SQL Query:

![5](https://github.com/user-attachments/assets/e0035631-768f-477d-998c-eb716ba5d8b6)


# General Transformations in the Gold Layer

The tables in the Gold layer are created from already refined data from the Silver layer. The objective is to aggregate data and calculate relevant metrics for strategic analysis.

## Joins:

Tables are integrated using primary and foreign keys to consolidate the data. For example, the `gold_clients` table joins `orders` and `order_items` by the `order_id` column.

## Aggregated Metrics:

Metrics are calculated using functions like `count`, `sum`, and complex mathematical operations.

## Renaming Columns:

Duplicate columns are renamed to avoid ambiguity during processing, such as renaming the `quantity` column from the `stocks` table.

## Storage in Delta Format:

Tables are stored in Delta format to ensure:
- ACID transactions.
- Version control.
- Support for incremental updates.

# Databricks Connection with Power BI

![6](https://github.com/user-attachments/assets/e722b7df-41e6-4a75-815d-4fa8b589ab91)

With structured tables, it is possible to expand the analysis and make it more accurate in Power BI.

## Steps:
1. Open Power BI Desktop and in the "Get Data" section, import the tables.
2. Configure the connector in Power BI:
   - **Server Hostname:** community.cloud.databricks.com
   - **HTTP Path:** Provided by the cluster in Databricks.
3. Create intermediate tables and connect to the database via JDBC/ODBC.

![7](https://github.com/user-attachments/assets/7d87f55b-f919-460a-bbc1-6663cea80ae7)

4. Display tables in Power BI for report generation.


---

## **How to Run This Repository**
1. **Set Up a Databricks Workspace**:
   - Create a cluster and attach the provided notebook.
2. **Upload Sample Data**:
   - Place the datasets in your workspace or an accessible storage location.
3. **Run the ETL Pipeline**:
   - Execute the notebook step by step to extract, transform, and load the data.
4. **Export Results**:
   - Use the built-in export commands to save the processed data in your desired format.
