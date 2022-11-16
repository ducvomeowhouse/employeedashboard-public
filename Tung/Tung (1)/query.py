# Databricks notebook source
# MAGIC %sql
# MAGIC USE SCHEMA TUNG_EMPLOYEEDB;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê số nhân viên mới theo tháng*/
# MAGIC SELECT
# MAGIC   COUNT(Employee_data_Tung.EmpID)
# MAGIC FROM
# MAGIC   Employee_data_Tung
# MAGIC WHERE
# MAGIC   YEAR(startDay) = 2022
# MAGIC   AND MONTH(StartDay) = MONTH(current_date());

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê số nhân viên mới theo  tuần*/
# MAGIC SELECT
# MAGIC   COUNT(Employee_data_Tung.EmpID)
# MAGIC FROM
# MAGIC   Employee_data_Tung
# MAGIC WHERE
# MAGIC   YEAR(startDay) = 2022
# MAGIC   AND MONTH(StartDay) = 7
# MAGIC   AND DAY(StartDay) BETWEEN 10 AND 20;
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê số tổng số nhân viên*/
# MAGIC SELECT
# MAGIC   COUNT(Employee_data_Tung.EmpID)
# MAGIC FROM
# MAGIC  Employee_data_Tung;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê số nhân viên thử việc*/
# MAGIC SELECT
# MAGIC   COUNT(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   ContractStatus = 'Probation';

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê số nhân viên chính thức*/
# MAGIC SELECT
# MAGIC   COUNT(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   ContractStatus = 'Infinity'
# MAGIC   OR ContractStatus = '1 Year';

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Cơ cấu nhân sự theo phòng ban*/
# MAGIC SELECT
# MAGIC   Tung_department.DepName,
# MAGIC   COUNT(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_department,
# MAGIC   Tung_employees
# MAGIC WHERE
# MAGIC   Tung_department.DepID = Tung_employees.DepID
# MAGIC GROUP BY
# MAGIC   Tung_department.DepName;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /*Cơ cấu nhân sự theo phòng ban đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_department.DepName,
# MAGIC   COUNT(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_department,
# MAGIC   Tung_employees,
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   Tung_department.DepID = Tung_employees.DepID
# MAGIC   AND Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_contract.ContractEndDay > current_date()
# MAGIC GROUP BY
# MAGIC   Tung_department.DepName;
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Cơ cấu nhân sự theo vị trí*/
# MAGIC SELECT
# MAGIC   Tung_role.RoleName,
# MAGIC   COUNT(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_role,
# MAGIC   Tung_employees
# MAGIC WHERE
# MAGIC   Tung_role.RoleID = Tung_employees.RoleID
# MAGIC GROUP BY
# MAGIC   Tung_role.RoleName;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Cơ cấu nhân sự theo vị trí đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_role.RoleName,
# MAGIC   COUNT(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_role,
# MAGIC   Tung_employees,
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   Tung_role.RoleID = Tung_employees.RoleID
# MAGIC   AND Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_contract.ContractEndDay > current_date()
# MAGIC GROUP BY
# MAGIC   Tung_role.RoleName;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê hợp đồng theo loại - tất cả đơn vị*/
# MAGIC SELECT
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   COUNT(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract,
# MAGIC   Tung_employees
# MAGIC WHERE
# MAGIC   Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC GROUP BY
# MAGIC   Tung_contract.ContractStatus;
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê hợp đồng theo loại - tất cả đơn vị - đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   COUNT(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract,
# MAGIC   Tung_employees
# MAGIC WHERE
# MAGIC   Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_contract.ContractStartDay < current_date()
# MAGIC GROUP BY
# MAGIC   Tung_contract.ContractStatus;
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê hợp đồng theo loại - theo từng đơn vị*/
# MAGIC SELECT
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   Tung_department.DepName,
# MAGIC   COUNT(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract,
# MAGIC   Tung_employees,
# MAGIC   Tung_department
# MAGIC WHERE
# MAGIC   Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_department.DepID = Tung_employees.DepID
# MAGIC GROUP BY
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   Tung_department.DepName;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Thống kê hợp đồng theo loại - theo từng đơn vị - đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   Tung_department.DepName,
# MAGIC   COUNT(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract,
# MAGIC   Tung_employees,
# MAGIC   Tung_department
# MAGIC WHERE
# MAGIC   Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_department.DepID = Tung_employees.DepID
# MAGIC   AND Tung_contract.ContractStartDay < current_date()
# MAGIC GROUP BY
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   Tung_department.DepName;

# COMMAND ----------

# MAGIC %sql  
# MAGIC /*Biến động nhân sự - đến năm hiện tại*/
# MAGIC SELECT
# MAGIC   YEAR(Tung_contract.ContractStartDay) AS YEARNOW,
# MAGIC   COUNT(
# MAGIC     CASE
# MAGIC       WHEN Tung_contract.ContractEndDay < current_date() then 1
# MAGIC     end
# MAGIC   ) AS EndEmp,
# MAGIC   COUNT(
# MAGIC     CASE
# MAGIC       WHEN Tung_contract.ContractStartDay < current_date() then 1
# MAGIC     end
# MAGIC   ) AS NewEmp
# MAGIC FROM
# MAGIC   Tung_employees,
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC   AND YEAR(Tung_contract.ContractStartDay) < year(current_date())
# MAGIC GROUP BY
# MAGIC   YEAR(Tung_contract.ContractStartDay);

# COMMAND ----------

# MAGIC %sql  
# MAGIC /*Biến động nhân sự theo phòng ban - đến năm hiện tại*/
# MAGIC SELECT
# MAGIC   YEAR(Tung_contract.ContractStartDay) AS YEARNOW,
# MAGIC   Tung_department.DepName,
# MAGIC   COUNT(
# MAGIC     CASE
# MAGIC       WHEN Tung_contract.ContractEndDay < current_date() then 1
# MAGIC     end
# MAGIC   ) AS EndEmp,
# MAGIC   COUNT(
# MAGIC     CASE
# MAGIC       WHEN Tung_contract.ContractStartDay < current_date() then 1
# MAGIC     end
# MAGIC   ) AS NewEmp
# MAGIC FROM
# MAGIC   Tung_employees,
# MAGIC   Tung_contract,
# MAGIC   Tung_department
# MAGIC WHERE
# MAGIC   Tung_department.DepID = Tung_employees.DepID
# MAGIC   AND Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC   AND YEAR(Tung_contract.ContractStartDay) < year(current_date())
# MAGIC GROUP BY
# MAGIC   YEAR(Tung_contract.ContractStartDay),
# MAGIC   Tung_department.DepName;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Biến động nhân sự theo vị trí - đến năm hiện tại*/
# MAGIC SELECT
# MAGIC   YEAR(Tung_contract.ContractStartDay) AS YEARNOW,
# MAGIC   Tung_role.RoleName,
# MAGIC   COUNT(
# MAGIC     CASE
# MAGIC       WHEN Tung_contract.ContractEndDay < current_date() then 1
# MAGIC     end
# MAGIC   ) AS EndEmp,
# MAGIC   COUNT(
# MAGIC     CASE
# MAGIC       WHEN Tung_contract.ContractStartDay < current_date() then 1
# MAGIC     end
# MAGIC   ) AS NewEmp
# MAGIC FROM
# MAGIC   Tung_employees,
# MAGIC   Tung_contract,
# MAGIC   Tung_role
# MAGIC WHERE
# MAGIC   Tung_role.RoleID = Tung_employees.RoleID
# MAGIC   AND Tung_contract.EmpID = Tung_employees.EmpID
# MAGIC   AND YEAR(Tung_contract.ContractStartDay) < year(current_date())
# MAGIC GROUP BY
# MAGIC   YEAR(Tung_contract.ContractStartDay),
# MAGIC   Tung_role.RoleName;
# MAGIC   

# COMMAND ----------

# MAGIC %sql    
# MAGIC /*Tổng chi phí lương cơ bản (Base salary) - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   SUM(Tung_empsalary.BaseSalary)
# MAGIC FROM
# MAGIC   Tung_empsalary;

# COMMAND ----------

# MAGIC %sql    
# MAGIC /*Tổng chi phí lương cơ bản (Base salary) - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   SUM(Tung_empsalary.BaseSalary)
# MAGIC FROM
# MAGIC   Tung_empsalary;
# MAGIC   
# MAGIC /*Tổng chi phí lương cơ bản (Base salary) theo từng đơn vị - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_department.DepName,
# MAGIC   SUM(Tung_empsalary.BaseSalary) AS salary
# MAGIC FROM
# MAGIC   Tung_empsalary,
# MAGIC   Tung_employees,
# MAGIC   Tung_department
# MAGIC WHERE
# MAGIC   Tung_empsalary.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_employees.DepID = Tung_department.DepID
# MAGIC GROUP BY
# MAGIC   Tung_department.DepName;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Tổng chi phí lương cơ bản (Base salary) theo từng vị trí - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_role.RoleName,
# MAGIC   SUM(Tung_empsalary.BaseSalary) AS salary
# MAGIC FROM
# MAGIC   Tung_empsalary,
# MAGIC   Tung_employees,
# MAGIC   Tung_role
# MAGIC WHERE
# MAGIC   Tung_empsalary.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_employees.RoleID = Tung_role.RoleID
# MAGIC GROUP BY
# MAGIC   Tung_role.RoleName;
# MAGIC   

# COMMAND ----------

# MAGIC %sql     
# MAGIC /*Tổng chi phí lương cơ bản (Gross salary) - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   SUM(Tung_empsalary.GrossSalary)
# MAGIC FROM
# MAGIC   Tung_empsalary;

# COMMAND ----------

# MAGIC %sql     
# MAGIC /*Tổng chi phí lương cơ bản (Gross salary) theo từng đơn vị - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_department.DepName,
# MAGIC   SUM(Tung_empsalary.GrossSalary) AS salary
# MAGIC FROM
# MAGIC   Tung_empsalary,
# MAGIC   Tung_employees,
# MAGIC   Tung_department
# MAGIC WHERE
# MAGIC   Tung_empsalary.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_employees.DepID = Tung_department.DepID
# MAGIC GROUP BY
# MAGIC   Tung_department.DepName;

# COMMAND ----------

# MAGIC %sql     
# MAGIC /*Tổng chi phí lương cơ bản (Gross salary) theo từng vị trí - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_role.RoleName,
# MAGIC   SUM(Tung_empsalary.GrossSalary) AS salary
# MAGIC FROM
# MAGIC   Tung_empsalary,
# MAGIC   Tung_employees,
# MAGIC   Tung_role
# MAGIC WHERE
# MAGIC   Tung_empsalary.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_employees.RoleID = Tung_role.RoleID
# MAGIC GROUP BY
# MAGIC   Tung_role.RoleName;

# COMMAND ----------

# MAGIC %sql     
# MAGIC /*Tổng chi phí lương cơ bản (Net salary) - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   SUM(Tung_empsalary.NetSalary)
# MAGIC FROM
# MAGIC   Tung_empsalary;

# COMMAND ----------

# MAGIC %sql       
# MAGIC /*Tổng chi phí lương cơ bản (Net salary) theo từng đơn vị - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_department.DepName,
# MAGIC   SUM(Tung_empsalary.NetSalary) AS salary
# MAGIC FROM
# MAGIC   Tung_empsalary,
# MAGIC   Tung_employees,
# MAGIC   Tung_department
# MAGIC WHERE
# MAGIC   Tung_empsalary.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_employees.DepID = Tung_department.DepID
# MAGIC GROUP BY
# MAGIC   Tung_department.DepName;
# MAGIC   

# COMMAND ----------

# MAGIC %sql       
# MAGIC   
# MAGIC /*Tổng chi phí lương cơ bản (Net salary) theo từng vị trí - Đến ngày hiện tại*/
# MAGIC SELECT
# MAGIC   Tung_role.RoleName,
# MAGIC   SUM(Tung_empsalary.NetSalary) AS salary
# MAGIC FROM
# MAGIC   Tung_empsalary,
# MAGIC   Tung_employees,
# MAGIC   Tung_role
# MAGIC WHERE
# MAGIC   Tung_empsalary.EmpID = Tung_employees.EmpID
# MAGIC   AND Tung_employees.RoleID = Tung_role.RoleID
# MAGIC GROUP BY
# MAGIC   Tung_role.RoleName;

# COMMAND ----------

# MAGIC %sql       
# MAGIC /*Thống kê hợp đồng theo loại*/
# MAGIC SELECT
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   count(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract
# MAGIC GROUP BY
# MAGIC   Tung_contract.ContractStatus;

# COMMAND ----------

# MAGIC %sql       
# MAGIC   
# MAGIC /*Thống kê hợp đồng - sắp hết hạn*/
# MAGIC SELECT
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   count(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   datediff(Tung_contract.ContractEndDay, current_date()) < 45
# MAGIC GROUP BY
# MAGIC   Tung_contract.ContractStatus;

# COMMAND ----------

# MAGIC %sql       
# MAGIC /*Thống kê hợp đồng hết hạn*/
# MAGIC SELECT
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   count(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   datediff(Tung_contract.ContractEndDay, current_date()) > 0
# MAGIC GROUP BY
# MAGIC   Tung_contract.ContractStatus;
# MAGIC   

# COMMAND ----------

# MAGIC %sql       
# MAGIC /*Thống kê nhân viên theo giới tính*/
# MAGIC SELECT
# MAGIC   Tung_employees.Gender,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_employees
# MAGIC GROUP BY
# MAGIC   Tung_employees.Gender;

# COMMAND ----------

# MAGIC %sql         
# MAGIC /*Thống kê nhân viên theo độ tuổi*/
# MAGIC SELECT
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_employees
# MAGIC WHERE
# MAGIC   ROUND(
# MAGIC     datediff(current_date(), Tung_employees.BirthDay) / 365,
# MAGIC     0
# MAGIC   ) BETWEEN 30 AND 45;

# COMMAND ----------

# MAGIC %sql         
# MAGIC /*Thống kê nhân viên theo thâm niên*/
# MAGIC SELECT
# MAGIC   count(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   datediff(Tung_contract.ContractEndDay, current_date()) < 0
# MAGIC   AND ROUND(
# MAGIC     datediff(current_date(), Tung_contract.ContractStartDay) / 365,
# MAGIC     0
# MAGIC   ) BETWEEN 0 AND 1;

# COMMAND ----------

# MAGIC %sql         
# MAGIC   
# MAGIC /*Thống kê hợp đồng hết hạn chưa tái kí*/
# MAGIC SELECT
# MAGIC   Tung_contract.ContractStatus,
# MAGIC   count(Tung_contract.EmpID)
# MAGIC FROM
# MAGIC   Tung_contract
# MAGIC WHERE
# MAGIC   datediff(Tung_contract.ContractEndDay, current_date()) > 0
# MAGIC GROUP BY
# MAGIC   Tung_contract.ContractStatus;

# COMMAND ----------

# MAGIC %sql         
# MAGIC /*Thống kê nhân viên theo vị trí*/
# MAGIC SELECT
# MAGIC   Tung_department.DepLocation,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_employees,
# MAGIC   Tung_department
# MAGIC WHERE
# MAGIC   Tung_employees.DepID = Tung_department.DepID
# MAGIC GROUP BY
# MAGIC   Tung_department.DepLocation;
# MAGIC   

# COMMAND ----------

# MAGIC %sql         
# MAGIC /*Tổng số dự án*/
# MAGIC SELECT
# MAGIC   count(Tung_projects.ProjectID)
# MAGIC FROM
# MAGIC   Tung_projects;
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC   
# MAGIC /*Số nhân sự theo từng dự án*/
# MAGIC SELECT
# MAGIC   Tung_projects.ProjectName,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_projects,
# MAGIC   Tung_employees
# MAGIC WHERE
# MAGIC   Tung_projects.ProjectID = Tung_employees.ProjectID
# MAGIC GROUP BY
# MAGIC   Tung_projects.ProjectName;

# COMMAND ----------

# MAGIC %sql         
# MAGIC /*Số nhân sự theo từng dự án theo từng vị trí*/
# MAGIC SELECT
# MAGIC   Tung_projects.ProjectName,
# MAGIC   Tung_role.RoleName,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_projects,
# MAGIC   Tung_employees,
# MAGIC   Tung_role
# MAGIC WHERE
# MAGIC   Tung_projects.ProjectID = Tung_employees.ProjectID
# MAGIC   AND Tung_employees.RoleID = Tung_role.RoleID
# MAGIC GROUP BY
# MAGIC   Tung_projects.ProjectName,
# MAGIC   Tung_role.RoleName;

# COMMAND ----------

# MAGIC %sql         
# MAGIC 
# MAGIC /*Tổng chi phí nhân sự (theo lương gross + thưởng) theo từng dự án*/
# MAGIC SELECT
# MAGIC   Tung_projects.ProjectName,
# MAGIC   sum(Tung_empsalary.GrossSalary),
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_projects,
# MAGIC   Tung_employees,
# MAGIC   Tung_empsalary
# MAGIC WHERE
# MAGIC   Tung_projects.ProjectID = Tung_employees.ProjectID
# MAGIC   AND Tung_employees.EmpID = Tung_empsalary.EmpID
# MAGIC GROUP BY
# MAGIC   Tung_projects.ProjectName;
# MAGIC   

# COMMAND ----------

# MAGIC %sql         
# MAGIC /*Tổng số nhân sự làm việc Online - theo từng dự án*/
# MAGIC SELECT
# MAGIC   Tung_projects.ProjectName,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_projects,
# MAGIC   Tung_employees,
# MAGIC   Tung_workingmodel
# MAGIC WHERE
# MAGIC   Tung_projects.ProjectID = Tung_employees.ProjectID
# MAGIC   AND Tung_employees.EmpID = Tung_workingmodel.EmpID
# MAGIC   AND Tung_workingmodel.WorkingTime = 'Online'
# MAGIC GROUP BY
# MAGIC   Tung_projects.ProjectName;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Tổng số nhân sự làm việc Offline - theo từng dự án*/
# MAGIC SELECT
# MAGIC   Tung_projects.ProjectName,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_projects,
# MAGIC   Tung_employees,
# MAGIC   Tung_workingmodel
# MAGIC WHERE
# MAGIC   Tung_projects.ProjectID = Tung_employees.ProjectID
# MAGIC   AND Tung_employees.EmpID = Tung_workingmodel.EmpID
# MAGIC   AND Tung_workingmodel.WorkingTime = 'Offline'
# MAGIC GROUP BY
# MAGIC   Tung_projects.ProjectName;

# COMMAND ----------

# MAGIC %sql        
# MAGIC /*Tổng số nhân sự làm việc Hybrid - theo từng dự án*/
# MAGIC SELECT
# MAGIC   Tung_projects.ProjectName,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_projects,
# MAGIC   Tung_employees,
# MAGIC   Tung_workingmodel
# MAGIC WHERE
# MAGIC   Tung_projects.ProjectID = Tung_employees.ProjectID
# MAGIC   AND Tung_employees.EmpID = Tung_workingmodel.EmpID
# MAGIC   AND Tung_workingmodel.WorkingTime = 'Hybrid'
# MAGIC GROUP BY
# MAGIC   Tung_projects.ProjectName;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Tổng số nhân sự làm việc Full time/Part time*/
# MAGIC SELECT
# MAGIC   Tung_workingmodel.WorkingModel,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_employees,
# MAGIC   Tung_workingmodel
# MAGIC WHERE
# MAGIC   Tung_employees.EmpID = Tung_workingmodel.EmpID
# MAGIC GROUP BY
# MAGIC   Tung_workingmodel.WorkingModel;
# MAGIC   

# COMMAND ----------

# MAGIC %sql          
# MAGIC /*Tổng số nhân viên theo effort */
# MAGIC SELECT
# MAGIC   Tung_workingmodel.Effort,
# MAGIC   count(Tung_employees.EmpID)
# MAGIC FROM
# MAGIC   Tung_employees,
# MAGIC   Tung_workingmodel
# MAGIC WHERE
# MAGIC   Tung_employees.EmpID = Tung_workingmodel.EmpID
# MAGIC GROUP BY
# MAGIC   Tung_workingmodel.Effort;