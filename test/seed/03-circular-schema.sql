-- Circular foreign key schema for testing
-- Creates a circular dependency: employees -> departments -> employees

USE production;

-- Departments table (created first, FK added later)
CREATE TABLE departments (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    budget DECIMAL(12, 2) DEFAULT 0,
    -- head_employee_id will be added after employees table exists
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Employees table with FK to departments and self-reference for manager
CREATE TABLE employees (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    department_id INT UNSIGNED,
    manager_id INT UNSIGNED,
    salary DECIMAL(10, 2) NOT NULL,
    hire_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (manager_id) REFERENCES employees(id)
) ENGINE=InnoDB;

-- Now add the circular FK from departments back to employees
ALTER TABLE departments
    ADD COLUMN head_employee_id INT UNSIGNED,
    ADD CONSTRAINT fk_dept_head FOREIGN KEY (head_employee_id) REFERENCES employees(id);

-- Projects table - employees can be assigned to projects
CREATE TABLE projects (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    department_id INT UNSIGNED NOT NULL,
    lead_employee_id INT UNSIGNED,
    budget DECIMAL(12, 2) DEFAULT 0,
    start_date DATE,
    end_date DATE,
    status ENUM('planning', 'active', 'completed', 'cancelled') DEFAULT 'planning',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (lead_employee_id) REFERENCES employees(id)
) ENGINE=InnoDB;

-- Project assignments - many-to-many between employees and projects
CREATE TABLE project_assignments (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    project_id INT UNSIGNED NOT NULL,
    employee_id INT UNSIGNED NOT NULL,
    role VARCHAR(50) NOT NULL,
    hours_allocated INT UNSIGNED DEFAULT 40,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (employee_id) REFERENCES employees(id),
    UNIQUE KEY unique_assignment (project_id, employee_id)
) ENGINE=InnoDB;
