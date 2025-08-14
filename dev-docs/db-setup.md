# Initial Database Setup Guide

This document outlines the steps to set up the `beancount` PostgreSQL database from scratch, including creating the database, setting up user roles with specific privileges, and initializing the schema.

These instructions assume you are using a tool like pgAdmin and have access to a PostgreSQL server with a user that has administrative privileges (e.g., a superuser or a user with the `CREATEDB` privilege).

## Overview

The goal of this setup is to create a secure environment with three distinct roles:

1.  **Owner (`sagar`):** An administrative role that owns the database and can perform any action, including creating and modifying the schema.
2.  **Writer (`beancount_loader`):** A restricted role for the Python application. It can read and write data but cannot delete data or alter the database structure.
3.  **Analyst (`beancount_analyst`):** A read-only role for analysis tools (including LLM agents). It can only read data, providing maximum safety.

---

## Step 1: Create the Database

Connect to your PostgreSQL server using an administrative account. Open a **Query Tool** for any existing database (like the default `postgres` database) and run the following command.

```sql
CREATE DATABASE beancount;
```

The user that executes this command becomes the owner of the new `beancount` database.

---

## Step 2: Create User Roles

Connect to the newly created `beancount` database as the owner (`sagar`) and open a new **Query Tool**. Run the following commands to create the two application-specific users. 

**Note:** Replace the placeholder passwords with strong, unique passwords and store them securely.

```sql
-- Create the user for the Python data loading script (writer)
CREATE USER beancount_loader WITH PASSWORD 'a-very-strong-password-for-loader';

-- Create the user for data analysis (read-only)
CREATE USER beancount_analyst WITH PASSWORD 'a-different-strong-password-for-analyst';
```

---

## Step 3: Create the Database Schema

In the same Query Tool (connected as the owner `sagar`), you will now create the database tables and indexes.

1.  Open the `create_schema.sql` file located in the root of this project.
2.  Copy the entire contents of the file.
3.  Paste the contents into the Query Tool and execute the script.

After the script completes, you can refresh the object browser in pgAdmin to see all the new tables under the `beancount` database.

---

## Step 4: Grant Role-Specific Privileges

After the tables are created, run the following commands as the owner (`sagar`) to grant the correct permissions to each new user.

#### A. Permissions for the Writer (`beancount_loader`)

This user can connect and read/write data, but cannot delete data or alter the schema.

```sql
-- Allow the user to connect to the database
GRANT CONNECT ON DATABASE beancount TO beancount_loader;

-- Allow the user to use the public schema
GRANT USAGE ON SCHEMA public TO beancount_loader;

-- Grant specific data manipulation privileges on all tables
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO beancount_loader;

-- Grant permissions for the ID-generating sequences
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO beancount_loader;
```

#### B. Permissions for the Analyst (`beancount_analyst`)

This user can connect and only read data.

```sql
-- Allow the user to connect to the database
GRANT CONNECT ON DATABASE beancount TO beancount_analyst;

-- Allow the user to use the public schema
GRANT USAGE ON SCHEMA public TO beancount_analyst;

-- Grant ONLY SELECT (read-only) privilege on all tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO beancount_analyst;
```

---

## Step 5: Verify Permissions

This is a crucial final step to ensure the security model is working.

1.  **Disconnect** from the server in pgAdmin.
2.  **Reconnect** as `beancount_analyst`.
3.  Open a Query Tool for the `beancount` database.
4.  **Test Read (Should Succeed):** `SELECT * FROM accounts LIMIT 5;`
5.  **Test Write (Should Fail):** `INSERT INTO tags (name) VALUES ('#test');` You should receive a `permission denied` error.

Then, repeat the process:

1.  **Disconnect** and **reconnect** as `beancount_loader`.
2.  Open a Query Tool for the `beancount` database.
3.  **Test Read/Write (Should Succeed):** `SELECT * FROM accounts;` and `INSERT INTO tags (name) VALUES ('#loader-test');`
4.  **Test Delete/Alter (Should Fail):** `DELETE FROM tags;` or `ALTER TABLE tags ADD COLUMN notes TEXT;` You should receive a `permission denied` error.

Once these tests pass, the database is correctly configured and ready for use.
