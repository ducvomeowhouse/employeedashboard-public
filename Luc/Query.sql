/*Thống kê số nhân viên mới theo tháng*/
SELECT
  COUNT(luc_employees.EmpID)
FROM
  luc_employees
WHERE
  YEAR(startDay) = 2022
  AND MONTH(StartDay) = MONTH(current_date());
  
/*Thống kê số nhân viên mới theo tuần*/
SELECT
  COUNT(luc_employees.EmpID)
FROM
  luc_employees
WHERE
  YEAR(startDay) = 2022
  AND MONTH(StartDay) = 7
  AND DAY(StartDay) BETWEEN 20 AND 28;
  
/*Thống kê số tổng số nhân viên*/
SELECT
  COUNT(luc_employees.EmpID)
FROM
  luc_employees;

/*Thống kê số nhân viên thử việc*/
SELECT
  COUNT(luc_contract.EmpID)
FROM
  luc_contract
WHERE
  ContractStatus = 'Probation';
  
/*Thống kê số nhân viên chính thức*/
SELECT
  COUNT(luc_contract.EmpID)
FROM
  luc_contract
WHERE
  ContractStatus = 'Infinity'
  OR ContractStatus = '1 Year';
  
/*Thống kê số nhân viên chính thức*/
SELECT
  COUNT(luc_contract.EmpID)
FROM
  luc_contract
WHERE
  NOT ContractStatus = 'Probation';
  
/*Cơ cấu nhân sự theo phòng ban*/
SELECT
  luc_department.DepName,
  COUNT(luc_employees.EmpID)
FROM
  luc_department,
  luc_employees
WHERE
  luc_department.DepID = luc_employees.DepID
GROUP BY
  luc_department.DepName;
  
/*Cơ cấu nhân sự theo phòng ban đến ngày hiện tại*/
SELECT
  luc_department.DepName,
  COUNT(luc_employees.EmpID)
FROM
  luc_department,
  luc_employees,
  luc_contract
WHERE
  luc_department.DepID = luc_employees.DepID
  AND luc_contract.EmpID = luc_employees.EmpID
  AND luc_contract.ContractEndDay >= current_date()
GROUP BY
  luc_department.DepName;
  
/*Cơ cấu nhân sự theo vị trí*/
SELECT
  luc_role.RoleName,
  COUNT(luc_employees.EmpID)
FROM
  luc_role,
  luc_employees
WHERE
  luc_role.RoleID = luc_employees.RoleID
GROUP BY
  luc_role.RoleName;
  
/*Cơ cấu nhân sự theo vị trí đến ngày hiện tại*/
SELECT
  luc_role.RoleName,
  COUNT(luc_employees.EmpID)
FROM
  luc_role,
  luc_employees,
  luc_contract
WHERE
  luc_role.RoleID = luc_employees.RoleID
  AND luc_contract.EmpID = luc_employees.EmpID
  AND luc_contract.ContractEndDay >= current_date()
GROUP BY
  luc_role.RoleName;
  
/*Thống kê hợp đồng theo loại - tất cả đơn vị*/
SELECT
  luc_contract.ContractStatus,
  COUNT(luc_contract.EmpID)
FROM
  luc_contract,
  luc_employees
WHERE
  luc_contract.EmpID = luc_employees.EmpID
GROUP BY
  luc_contract.ContractStatus;
  
/*Thống kê hợp đồng theo loại - tất cả đơn vị - đến ngày hiện tại*/
SELECT
  luc_contract.ContractStatus,
  COUNT(luc_contract.EmpID)
FROM
  luc_contract,
  luc_employees
WHERE
  luc_contract.EmpID = luc_employees.EmpID
  AND luc_contract.ContractStartDay =< current_date()
GROUP BY
  luc_contract.ContractStatus;
  
/*Thống kê hợp đồng theo loại - theo từng đơn vị*/
SELECT
  luc_contract.ContractStatus,
  luc_department.DepName,
  COUNT(luc_contract.EmpID)
FROM
  luc_contract,
  luc_employees,
  luc_department
WHERE
  luc_contract.EmpID = luc_employees.EmpID
  AND luc_department.DepID = luc_employees.DepID
GROUP BY
  luc_contract.ContractStatus,
  luc_department.DepName;
  
/*Thống kê hợp đồng theo loại - theo từng đơn vị - đến ngày hiện tại*/
SELECT
  luc_contract.ContractStatus,
  luc_department.DepName,
  COUNT(luc_contract.EmpID)
FROM
  luc_contract,
  luc_employees,
  luc_department
WHERE
  luc_contract.EmpID = luc_employees.EmpID
  AND luc_department.DepID = luc_employees.DepID
  AND luc_contract.ContractStartDay =< current_date()
GROUP BY
  luc_contract.ContractStatus,
  luc_department.DepName;
  
/*Biến động nhân sự - đến năm hiện tại*/
SELECT
  YEAR(luc_contract.ContractStartDay) AS YEARNOW,
  COUNT(
    CASE
      WHEN luc_contract.ContractEndDay < current_date() then 1
    end
  ) AS EndEmp,
  COUNT(
    CASE
      WHEN luc_contract.ContractStartDay < current_date() then 1
    end
  ) AS NewEmp
FROM
  luc_employees,
  luc_contract
WHERE
  luc_contract.EmpID = luc_employees.EmpID
  AND YEAR(luc_contract.ContractStartDay) < year(current_date())
GROUP BY
  YEAR(luc_contract.ContractStartDay);
  
/*Biến động nhân sự theo phòng ban - đến năm hiện tại*/
SELECT
  YEAR(luc_contract.ContractStartDay) AS YEARNOW,
  luc_department.DepName,
  COUNT(
    CASE
      WHEN luc_contract.ContractEndDay < current_date() then 1
    end
  ) AS EndEmp,
  COUNT(
    CASE
      WHEN luc_contract.ContractStartDay < current_date() then 1
    end
  ) AS NewEmp
FROM
  luc_employees,
  luc_contract,
  luc_department
WHERE
  luc_department.DepID = luc_employees.DepID
  AND luc_contract.EmpID = luc_employees.EmpID
  AND YEAR(luc_contract.ContractStartDay) < year(current_date())
GROUP BY
  YEAR(luc_contract.ContractStartDay),
  luc_department.DepName;
  
/*Biến động nhân sự theo vị trí - đến năm hiện tại*/
SELECT
  YEAR(luc_contract.ContractStartDay) AS YEARNOW,
  luc_role.RoleName,
  COUNT(
    CASE
      WHEN luc_contract.ContractEndDay < current_date() then 1
    end
  ) AS EndEmp,
  COUNT(
    CASE
      WHEN luc_contract.ContractStartDay < current_date() then 1
    end
  ) AS NewEmp
FROM
  luc_employees,
  luc_contract,
  luc_role
WHERE
  luc_role.RoleID = luc_employees.RoleID
  AND luc_contract.EmpID = luc_employees.EmpID
  AND YEAR(luc_contract.ContractStartDay) < year(current_date())
GROUP BY
  YEAR(luc_contract.ContractStartDay),
  luc_role.RoleName;
  
/*Tổng chi phí lương cơ bản (Base salary) - Đến ngày hiện tại*/
SELECT
  SUM(luc_empsalary.BaseSalary)
FROM
  luc_empsalary;
  
/*Tổng chi phí lương cơ bản (Base salary) theo từng đơn vị - Đến ngày hiện tại*/
SELECT
  luc_department.DepName,
  SUM(luc_empsalary.BaseSalary) AS salary
FROM
  luc_empsalary,
  luc_employees,
  luc_department
WHERE
  luc_empsalary.EmpID = luc_employees.EmpID
  AND luc_employees.DepID = luc_department.DepID
GROUP BY
  luc_department.DepName;
  
/*Tổng chi phí lương cơ bản (Base salary) theo từng vị trí - Đến ngày hiện tại*/
SELECT
  luc_role.RoleName,
  SUM(luc_empsalary.BaseSalary) AS salary
FROM
  luc_empsalary,
  luc_employees,
  luc_role
WHERE
  luc_empsalary.EmpID = luc_employees.EmpID
  AND luc_employees.RoleID = luc_role.RoleID
GROUP BY
  luc_role.RoleName;
  
/*Tổng chi phí lương cơ bản (Gross salary) - Đến ngày hiện tại*/
SELECT
  SUM(luc_empsalary.GrossSalary)
FROM
  luc_empsalary;
  
/*Tổng chi phí lương cơ bản (Gross salary) theo từng đơn vị - Đến ngày hiện tại*/
SELECT
  luc_department.DepName,
  SUM(luc_empsalary.GrossSalary) AS salary
FROM
  luc_empsalary,
  luc_employees,
  luc_department
WHERE
  luc_empsalary.EmpID = luc_employees.EmpID
  AND luc_employees.DepID = luc_department.DepID
GROUP BY
  luc_department.DepName;
  
/*Tổng chi phí lương cơ bản (Gross salary) theo từng vị trí - Đến ngày hiện tại*/
SELECT
  luc_role.RoleName,
  SUM(luc_empsalary.GrossSalary) AS salary
FROM
  luc_empsalary,
  luc_employees,
  luc_role
WHERE
  luc_empsalary.EmpID = luc_employees.EmpID
  AND luc_employees.RoleID = luc_role.RoleID
GROUP BY
  luc_role.RoleName;
  
/*Tổng chi phí lương cơ bản (Net salary) - Đến ngày hiện tại*/
SELECT
  SUM(luc_empsalary.NetSalary)
FROM
  luc_empsalary;
  
/*Tổng chi phí lương cơ bản (Net salary) theo từng đơn vị - Đến ngày hiện tại*/
SELECT
  luc_department.DepName,
  SUM(luc_empsalary.NetSalary) AS salary
FROM
  luc_empsalary,
  luc_employees,
  luc_department
WHERE
  luc_empsalary.EmpID = luc_employees.EmpID
  AND luc_employees.DepID = luc_department.DepID
GROUP BY
  luc_department.DepName;
  
/*Tổng chi phí lương cơ bản (Net salary) theo từng vị trí - Đến ngày hiện tại*/
SELECT
  luc_role.RoleName,
  SUM(luc_empsalary.NetSalary) AS salary
FROM
  luc_empsalary,
  luc_employees,
  luc_role
WHERE
  luc_empsalary.EmpID = luc_employees.EmpID
  AND luc_employees.RoleID = luc_role.RoleID
GROUP BY
  luc_role.RoleName;
  
/*Thống kê hợp đồng theo loại*/
SELECT
  luc_contract.ContractStatus,
  count(luc_contract.EmpID)
FROM
  luc_contract
GROUP BY
  luc_contract.ContractStatus;
  
/*Thống kê hợp đồng - sắp hết hạn*/
SELECT
  luc_contract.ContractStatus,
  count(luc_contract.EmpID)
FROM
  luc_contract
WHERE
  datediff(luc_contract.ContractEndDay, current_date()) < 45
GROUP BY
  luc_contract.ContractStatus;
  
/*Thống kê hợp đồng hết hạn*/
SELECT
  luc_contract.ContractStatus,
  count(luc_contract.EmpID)
FROM
  luc_contract
WHERE
  datediff(luc_contract.ContractEndDay, current_date()) > 0
GROUP BY
  luc_contract.ContractStatus;
  
/*Thống kê nhân viên theo giới tính*/
SELECT
  luc_employees.Gender,
  count(luc_employees.EmpID)
FROM
  luc_employees
GROUP BY
  luc_employees.Gender;
  
/*Thống kê nhân viên theo độ tuổi*/
SELECT
  count(luc_employees.EmpID)
FROM
  luc_employees
WHERE
  ROUND(
    datediff(current_date(), luc_employees.BirthDay) / 365,
    0
  ) BETWEEN 30 AND 45;
  
/*Thống kê nhân viên theo thâm niên*/
SELECT
  count(luc_contract.EmpID)
FROM
  luc_contract
WHERE
  datediff(luc_contract.ContractEndDay, current_date()) < 0
  AND ROUND(
    datediff(current_date(), luc_contract.ContractStartDay) / 365,
    0
  ) BETWEEN 0 AND 1;
  
/*Thống kê hợp đồng hết hạn chưa tái kí*/
SELECT
  luc_contract.ContractStatus,
  count(luc_contract.EmpID)
FROM
  luc_contract
WHERE
  datediff(luc_contract.ContractEndDay, current_date()) > 0
GROUP BY
  luc_contract.ContractStatus;
  
/*Thống kê nhân viên theo vị trí*/
SELECT
  luc_department.DepLocation,
  count(luc_employees.EmpID)
FROM
  luc_employees,
  luc_department
WHERE
  luc_employees.DepID = luc_department.DepID
GROUP BY
  luc_department.DepLocation;
  
/*Tổng số dự án*/
SELECT
  count(luc_projects.ProjectID)
FROM
  luc_projects;
  
/*Số nhân sự theo từng dự án*/
SELECT
  luc_projects.ProjectName,
  count(luc_employees.EmpID)
FROM
  luc_projects,
  luc_employees
WHERE
  luc_projects.ProjectID = luc_employees.ProjectID
GROUP BY
  luc_projects.ProjectName;
  
/*Số nhân sự theo từng dự án theo từng vị trí*/
SELECT
  luc_projects.ProjectName,
  luc_role.RoleName,
  count(luc_employees.EmpID)
FROM
  luc_projects,
  luc_employees,
  luc_role
WHERE
  luc_projects.ProjectID = luc_employees.ProjectID
  AND luc_employees.RoleID = luc_role.RoleID
GROUP BY
  luc_projects.ProjectName,
  luc_role.RoleName;
  
/*Tổng chi phí nhân sự (theo lương gross + thưởng) theo từng dự án*/
SELECT
  luc_projects.ProjectName,
  sum(luc_empsalary.GrossSalary),
  count(luc_employees.EmpID)
FROM
  luc_projects,
  luc_employees,
  luc_empsalary
WHERE
  luc_projects.ProjectID = luc_employees.ProjectID
  AND luc_employees.EmpID = luc_empsalary.EmpID
GROUP BY
  luc_projects.ProjectName;
  
/*Tổng số nhân sự làm việc Online - theo từng dự án*/
SELECT
  luc_projects.ProjectName,
  count(luc_employees.EmpID)
FROM
  luc_projects,
  luc_employees,
  luc_workingmodel
WHERE
  luc_projects.ProjectID = luc_employees.ProjectID
  AND luc_employees.EmpID = luc_workingmodel.EmpID
  AND luc_workingmodel.WorkingTime = 'Online'
GROUP BY
  luc_projects.ProjectName;
  
/*Tổng số nhân sự làm việc Offline - theo từng dự án*/
SELECT
  luc_projects.ProjectName,
  count(luc_employees.EmpID)
FROM
  luc_projects,
  luc_employees,
  luc_workingmodel
WHERE
  luc_projects.ProjectID = luc_employees.ProjectID
  AND luc_employees.EmpID = luc_workingmodel.EmpID
  AND luc_workingmodel.WorkingTime = 'Offline'
GROUP BY
  luc_projects.ProjectName;
  
/*Tổng số nhân sự làm việc Hybrid - theo từng dự án*/
SELECT
  luc_projects.ProjectName,
  count(luc_employees.EmpID)
FROM
  luc_projects,
  luc_employees,
  luc_workingmodel
WHERE
  luc_projects.ProjectID = luc_employees.ProjectID
  AND luc_employees.EmpID = luc_workingmodel.EmpID
  AND luc_workingmodel.WorkingTime = 'Hybrid'
GROUP BY
  luc_projects.ProjectName;
  
/*Tổng số nhân sự làm việc Full time/Part time*/
SELECT
  luc_workingmodel.WorkingModel,
  count(luc_employees.EmpID)
FROM
  luc_employees,
  luc_workingmodel
WHERE
  luc_employees.EmpID = luc_workingmodel.EmpID
GROUP BY
  luc_workingmodel.WorkingModel;
  
/*Tổng số nhân viên theo effort */
SELECT
  luc_workingmodel.Effort,
  count(luc_employees.EmpID)
FROM
  luc_employees,
  luc_workingmodel
WHERE
  luc_employees.EmpID = luc_workingmodel.EmpID
GROUP BY
  luc_workingmodel.Effort;