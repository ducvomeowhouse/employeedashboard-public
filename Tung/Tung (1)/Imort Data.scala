// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

// MAGIC %sql
// MAGIC drop schema TUNG_EMPLOYEEDB CASCADE;

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE SCHEMA IF NOT EXISTS TUNG_EMPLOYEEDB;

// COMMAND ----------

// MAGIC %sql
// MAGIC USE SCHEMA TUNG_EMPLOYEEDB;

// COMMAND ----------

// MAGIC %scala
// MAGIC import scala.sys.process._
// MAGIC 
// MAGIC // Choose a name for your resulting table in Databricks
// MAGIC var tableName = "Employee_data_Tung"
// MAGIC 
// MAGIC // Replace this URL with the one from your Google Spreadsheets
// MAGIC // Click on File --> Publish to the Web --> Option CSV and copy the URL
// MAGIC var url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRVTIDfAP7SjVTCVRgZ8jEEsjVd2-AHGuCavDaZvQRNRMCYwihLcYVIK3tHi5LU_PYyp5uA6THOVNIT/pub?output=csv"
// MAGIC 
// MAGIC var localpath = "/tmp/" + tableName + ".csv"
// MAGIC dbutils.fs.rm("file:" + localpath)
// MAGIC "wget -O " + localpath + " " + url !!
// MAGIC 
// MAGIC dbutils.fs.mkdirs("dbfs:/datasets/gsheets")
// MAGIC dbutils.fs.cp("file:" + localpath, "dbfs:/datasets/gsheets")
// MAGIC 
// MAGIC sqlContext.sql("drop table if exists " + tableName)
// MAGIC var df = spark.read.option("header", "true").option("inferSchema", "true").csv("/datasets/gsheets/" + tableName + ".csv");
// MAGIC 
// MAGIC df.write.saveAsTable(tableName);

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from Employee_data_Tung

// COMMAND ----------

// MAGIC %python
// MAGIC # With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
// MAGIC # Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
// MAGIC # To do so, choose your table name and uncomment the bottom line.
// MAGIC 
// MAGIC permanent_table_name = ""
// MAGIC 
// MAGIC # df.write.format("parquet").saveAsTable(permanent_table_name)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_Bank(
// MAGIC BankAccountNumber STRING,
// MAGIC BankName STRING
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Tung_Bank
// MAGIC SELECT BankAccountNumber, BankName
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Tung_Bank

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_Role(
// MAGIC RoleID INT,
// MAGIC RoleName STRING
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Tung_Role
// MAGIC SELECT DISTINCT RoleID, RoleName 
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_contract(
// MAGIC EmpID INT,
// MAGIC ContractStatus STRING,
// MAGIC ContractStartDay STRING,
// MAGIC ContractEndDay STRING
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Tung_contract
// MAGIC SELECT EmpID, ContractStatus,ContractStartDay,ContractEndDay
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_department(
// MAGIC DepID INT,
// MAGIC DepName STRING,
// MAGIC DepLocation STRING
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO  Tung_department
// MAGIC SELECT DISTINCT DepID, Department,DepLocation
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Tung_department

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_EmpCV(
// MAGIC EmpID INT,
// MAGIC LinkCV STRING
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO  Tung_EmpCV
// MAGIC SELECT EmpID, LinkCV
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Tung_EmpCV

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_EmpSalary(
// MAGIC EmpID INT,
// MAGIC BaseSalary DOUBLE,
// MAGIC GrossSalary DOUBLE,
// MAGIC NetSalary DOUBLE
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Tung_EmpSalary
// MAGIC SELECT EmpID, BaseSalary,GrossSalary,NetSalary
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Tung_EmpSalary

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_WorkingModel(
// MAGIC EmpID INT,
// MAGIC WorkingModel STRING,
// MAGIC Effort INT,
// MAGIC WorkingTime STRING
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Tung_WorkingModel
// MAGIC SELECT EmpID, WorkingModel,Effort,WorkingTime
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Tung_WorkingModel

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_Projects(
// MAGIC ProjectID INT,
// MAGIC ProjectName STRING,
// MAGIC ProjectManager STRING,
// MAGIC ProjectStartDay DATE,
// MAGIC ProjectEndDay DATE,
// MAGIC ProjectStatus STRING,
// MAGIC ProjectBudget DOUBLE
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Tung_Projects
// MAGIC SELECT DISTINCT ProjectID, ProjectName,ProjectManager,ProjectStartDay,ProjectEndDay,ProjectStatus,Projectbudget
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Tung_Projects

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE Tung_Employees(
// MAGIC EmpID INT,
// MAGIC Name STRING,
// MAGIC Gender STRING,
// MAGIC EmpEmail STRING,
// MAGIC Phone STRING,
// MAGIC FaxNumber STRING,
// MAGIC BirthDay DATE,
// MAGIC University STRING,
// MAGIC StartDay DATE,
// MAGIC OfficialDay DATE,
// MAGIC CompanyEmail STRING,
// MAGIC CitizenIdentification STRING,
// MAGIC DateCreated DATE,
// MAGIC DateModified DATE,
// MAGIC ProjectID INT, 
// MAGIC DepID INT,
// MAGIC BankAccountNumber STRING,
// MAGIC RoleID INT
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Tung_Employees
// MAGIC SELECT DISTINCT EmpID, Name,Gender,EmpEmail,EmpPhone,FaxNumber,BirthDay, University, StartDay, OfficialDay, CompanyEmail, CitizenIdentification, DateCreated, DateModified, ProjectID, DepID, BankAccountNumber, RoleID
// MAGIC FROM Employee_data_Tung

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM Tung_Employees