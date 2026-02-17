-- Seed data for circular foreign key testing
-- Must handle the chicken-and-egg problem of circular FKs

USE org_chart;

-- Temporarily disable FK checks to insert circular data
SET FOREIGN_KEY_CHECKS = 0;

-- Insert departments (head_employee_id set to NULL initially)
INSERT INTO departments (id, name, budget, head_employee_id) VALUES
(1, 'Engineering', 5000000.00, NULL),
(2, 'Product', 2000000.00, NULL),
(3, 'Sales', 3000000.00, NULL),
(4, 'HR', 1000000.00, NULL);

-- Insert employees with department and manager relationships
-- CEO has no manager (NULL manager_id)
INSERT INTO employees (id, name, email, department_id, manager_id, salary, hire_date) VALUES
-- CEO - no manager, in Engineering
(1, 'Sarah Chen', 'sarah.chen@company.com', 1, NULL, 350000.00, '2015-01-15'),

-- VPs report to CEO
(2, 'Mike Johnson', 'mike.johnson@company.com', 1, 1, 250000.00, '2016-03-01'),
(3, 'Lisa Park', 'lisa.park@company.com', 2, 1, 250000.00, '2016-06-15'),
(4, 'Tom Wilson', 'tom.wilson@company.com', 3, 1, 240000.00, '2017-01-10'),
(5, 'Anna Garcia', 'anna.garcia@company.com', 4, 1, 220000.00, '2017-04-20'),

-- Engineering team (reports to Mike)
(6, 'David Lee', 'david.lee@company.com', 1, 2, 180000.00, '2018-02-01'),
(7, 'Emma White', 'emma.white@company.com', 1, 2, 175000.00, '2018-05-15'),
(8, 'James Brown', 'james.brown@company.com', 1, 6, 150000.00, '2019-01-10'),
(9, 'Sophie Taylor', 'sophie.taylor@company.com', 1, 6, 145000.00, '2019-03-20'),
(10, 'Chris Martin', 'chris.martin@company.com', 1, 7, 140000.00, '2019-06-01'),

-- Product team (reports to Lisa)
(11, 'Rachel Green', 'rachel.green@company.com', 2, 3, 160000.00, '2018-04-01'),
(12, 'Kevin Moore', 'kevin.moore@company.com', 2, 3, 155000.00, '2018-07-15'),
(13, 'Mia Clark', 'mia.clark@company.com', 2, 11, 130000.00, '2019-09-01'),

-- Sales team (reports to Tom)
(14, 'Ryan Davis', 'ryan.davis@company.com', 3, 4, 170000.00, '2018-01-15'),
(15, 'Olivia Miller', 'olivia.miller@company.com', 3, 4, 165000.00, '2018-08-01'),
(16, 'Jack Anderson', 'jack.anderson@company.com', 3, 14, 120000.00, '2020-01-10'),

-- HR team (reports to Anna)
(17, 'Emily Wright', 'emily.wright@company.com', 4, 5, 130000.00, '2018-09-01'),
(18, 'Noah King', 'noah.king@company.com', 4, 5, 125000.00, '2019-11-15');

-- Now set department heads (creating the circular references)
UPDATE departments SET head_employee_id = 2 WHERE id = 1;  -- Mike heads Engineering
UPDATE departments SET head_employee_id = 3 WHERE id = 2;  -- Lisa heads Product
UPDATE departments SET head_employee_id = 4 WHERE id = 3;  -- Tom heads Sales
UPDATE departments SET head_employee_id = 5 WHERE id = 4;  -- Anna heads HR

-- Insert projects
INSERT INTO projects (id, name, department_id, lead_employee_id, budget, start_date, end_date, status) VALUES
(1, 'Platform Redesign', 1, 6, 500000.00, '2024-01-01', '2024-06-30', 'active'),
(2, 'Mobile App v2', 1, 7, 300000.00, '2024-02-01', '2024-08-31', 'active'),
(3, 'Customer Portal', 2, 11, 200000.00, '2024-03-01', '2024-09-30', 'planning'),
(4, 'Sales Dashboard', 3, 14, 150000.00, '2024-01-15', '2024-05-15', 'active'),
(5, 'HR System Upgrade', 4, 17, 100000.00, '2024-04-01', '2024-07-31', 'planning'),
(6, 'Legacy Migration', 1, 2, 800000.00, '2023-06-01', '2024-03-31', 'completed');

-- Insert project assignments
INSERT INTO project_assignments (project_id, employee_id, role, hours_allocated) VALUES
-- Platform Redesign team
(1, 6, 'Tech Lead', 40),
(1, 8, 'Senior Developer', 40),
(1, 9, 'Developer', 40),
(1, 11, 'Product Manager', 20),

-- Mobile App v2 team
(2, 7, 'Tech Lead', 40),
(2, 10, 'Developer', 40),
(2, 12, 'Product Manager', 20),

-- Customer Portal team
(3, 11, 'Product Lead', 30),
(3, 13, 'Product Manager', 40),
(3, 8, 'Developer', 20),

-- Sales Dashboard team
(4, 14, 'Project Lead', 30),
(4, 15, 'Sales Lead', 20),
(4, 9, 'Developer', 30),

-- HR System Upgrade team
(5, 17, 'Project Lead', 40),
(5, 18, 'HR Specialist', 40),
(5, 10, 'Developer', 20),

-- Legacy Migration team (completed project)
(6, 2, 'Executive Sponsor', 10),
(6, 6, 'Tech Lead', 40),
(6, 7, 'Architect', 30),
(6, 8, 'Developer', 40),
(6, 9, 'Developer', 40);

-- Re-enable FK checks
SET FOREIGN_KEY_CHECKS = 1;
