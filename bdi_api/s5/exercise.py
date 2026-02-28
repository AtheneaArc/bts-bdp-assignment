from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings
#Use psycopg2 for direct SQL execution and for simplicity
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path

settings = Settings()

#To get the database connection that is in settings
def get_connection():
    return psycopg2.connect(settings.db_url)

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["s5"],
)


@s5.post("/db/init")
def init_database() -> str:
    """Create all HR database tables (department, employee, project,
    employee_project, salary_history) with their relationships and indexes.

    Use the BDI_DB_URL environment variable to configure the database connection.
    Default: sqlite:///hr_database.db
    """
    #Connect to the database using psycopg2
    conn=psycopg2.connect(settings.db_url)
    #create a cursor to execute SQL commands
    cur=conn.cursor()
    #DROP TABLES IF THEY EXIST (for idempotency)
    cur.execute("""
                DROP TABLE IF EXISTS salary_history CASCADE;
                DROP TABLE IF EXISTS employee_project CASCADE;
                DROP TABLE IF EXISTS employee CASCADE;
                DROP TABLE IF EXISTS project CASCADE;
                DROP TABLE IF EXISTS department CASCADE;""")
    conn.commit()
    #Execute the schema creation SQL (see hr_schema.sql)
    schema_path=Path(__file__).parent / "hr_schema.sql"
    #Open the schema file and execute its contents
    with open(schema_path,"r",encoding="utf-8") as f:
        schema_sql=f.read()
    #Execute the SQL commands to create tables and indexes
    cur.execute(schema_sql)
    conn.commit()
    #Close the cursor and connection
    cur.close()
    conn.close()
    return "OK"


@s5.post("/db/seed")
def seed_database() -> str:
    """Populate the HR database with sample data.

    Inserts departments, employees, projects, assignments, and salary history.
    """
    #Connect to the database
    conn=psycopg2.connect(settings.db_url)
    cur=conn.cursor()
    #DELETE ALL EXISTING DATA (for idempotency)
    cur.execute("""
                DELETE FROM salary_history;
                DELETE FROM employee_project;
                DELETE FROM employee;
                DELETE FROM project;
                DELETE FROM department;""")
    conn.commit()

    #Execute the seed data SQL (see hr_seed_data.sql)
    seed_path=Path(__file__).parent / "hr_seed_data.sql"
    #Open the seed data file and execute its contents
    with open(seed_path,"r",encoding="utf-8") as f:
        seed_sql=f.read()
    #Execute the SQL commands to insert sample data
    cur.execute(seed_sql)
    conn.commit()
    #Close the cursor and connection
    cur.close()
    conn.close()
    return "OK"


@s5.get("/departments/")
def list_departments() -> list[dict]:
    """Return all departments.

    Each department should include: id, name, location
    """
    #Query all departments and return as list of dicts
    conn=psycopg2.connect(settings.db_url)
    cur=conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, name, location FROM department")
    departments=cur.fetchall()
    cur.close()
    conn.close()
    return departments



@s5.get("/employees/")
def list_employees(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    per_page: Annotated[
        int,
        Query(description="Number of employees per page", ge=1, le=100),
    ] = 10,
) -> list[dict]:
    """Return employees with their department name, paginated.

    Each employee should include: id, first_name, last_name, email, salary, department_name
    """
    #Query employees with JOIN to department, apply OFFSET and LIMIT
    offset=(page-1)*per_page
    conn=psycopg2.connect(settings.db_url)
    cur=conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
                SELECT e.id,e.first_name,e.last_name,e.email,e.salary,d.name AS department_name
                FROM employee e
                JOIN department d ON e.department_id=d.id
                ORDER BY e.id
                LIMIT %s OFFSET %s""",
                (per_page, offset))
    employees=cur.fetchall()
    cur.close()
    conn.close()
    return employees


@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    """Return all employees in a specific department.

    Each employee should include: id, first_name, last_name, email, salary, hire_date
    """
    #Query employees filtered by department_id
    conn=psycopg2.connect(settings.db_url)
    cur=conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
                SELECT id,first_name,last_name, email, salary, hire_date
                FROM employee
                WHERE department_id=%s
                ORDER BY id""",
                (dept_id,))
    department_employees=cur.fetchall()
    cur.close()
    conn.close()
    return department_employees or {} #Return empty dict if no employees found


@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    """Return KPI statistics for a department.

    Response should include: department_name, employee_count, avg_salary, project_count
    """
    #Calculate department statistics using JOINs and aggregations
    conn=psycopg2.connect(settings.db_url)
    cur=conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
                SELECT d.name AS department_name,
                          COUNT(e.id) AS employee_count,
                          AVG(e.salary) AS avg_salary,
                          COUNT(DISTINCT ep.project_id) AS project_count
                FROM department d
                LEFT JOIN employee e ON e.department_id=d.id
                LEFT JOIN employee_project ep ON ep.employee_id=e.id
                WHERE d.id=%s
                GROUP BY d.id, d.name""",
                (dept_id,))


    department_stats=cur.fetchone()
    cur.close()
    conn.close()
    return department_stats


@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    """Return the salary evolution for an employee, ordered by date.

    Each entry should include: change_date, old_salary, new_salary, reason
    """
    #Query salary_history for the given employee, ordered by change_date
    conn=psycopg2.connect(settings.db_url)
    cur=conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
                SELECT change_date, old_salary, new_salary, reason
                FROM salary_history
                WHERE employee_id=%s
                ORDER BY change_date""",
                (emp_id,))
    salary_history=cur.fetchall()
    cur.close()
    conn.close()
    return salary_history
