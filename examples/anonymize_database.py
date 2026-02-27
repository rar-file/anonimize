#!/usr/bin/env python3
"""Example: Anonymize database tables with PII data.

This example demonstrates how to use the DatabaseAnonymizer to anonymize
personally identifiable information in database tables. It creates a
SQLite database for demonstration, but the same code works with
PostgreSQL and MySQL by changing the connection string.
"""

import os
import tempfile
from pathlib import Path

# SQLAlchemy for database operations
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import declarative_base, sessionmaker

from anonimize.anonymizers.database import DatabaseAnonymizer

Base = declarative_base()


class User(Base):
    """Example User table."""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    phone = Column(String(20))
    address = Column(Text)
    company = Column(String(100))


class Order(Base):
    """Example Order table with foreign key reference."""
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    customer_email = Column(String(100))
    shipping_address = Column(Text)


def setup_database(db_path: str) -> None:
    """Create sample database with test data.
    
    Args:
        db_path: Path to SQLite database file.
    """
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Add sample users
    users = [
        User(name="John Doe", email="john@example.com", phone="555-123-4567", 
             address="123 Main St, New York, NY", company="Acme Corp"),
        User(name="Jane Smith", email="jane@company.org", phone="555-987-6543",
             address="456 Oak Ave, Los Angeles, CA", company="Tech Solutions"),
        User(name="Bob Johnson", email="bob@test.net", phone="555-456-7890",
             address="789 Pine Rd, Chicago, IL", company="Data Systems"),
        User(name="Alice Williams", email="alice@enterprise.com", phone="555-234-5678",
             address="321 Elm St, Houston, TX", company="Cloud Services"),
        User(name="Charlie Brown", email="charlie@mail.com", phone="555-876-5432",
             address="654 Maple Dr, Phoenix, AZ", company="Innovation Labs"),
    ]
    
    session.add_all(users)
    session.commit()
    
    # Add sample orders
    orders = [
        Order(user_id=1, customer_email="john@example.com", shipping_address="123 Main St, New York, NY"),
        Order(user_id=1, customer_email="john@example.com", shipping_address="123 Main St, New York, NY"),
        Order(user_id=2, customer_email="jane@company.org", shipping_address="456 Oak Ave, Los Angeles, CA"),
        Order(user_id=3, customer_email="bob@test.net", shipping_address="789 Pine Rd, Chicago, IL"),
        Order(user_id=4, customer_email="alice@enterprise.com", shipping_address="321 Elm St, Houston, TX"),
    ]
    
    session.add_all(orders)
    session.commit()
    session.close()
    
    print(f"Created sample database: {db_path}")


def display_table(engine, table_name: str, limit: int = 5) -> None:
    """Display contents of a database table.
    
    Args:
        engine: SQLAlchemy engine.
        table_name: Name of the table to display.
        limit: Maximum number of rows to show.
    """
    from sqlalchemy import text
    
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
        rows = result.fetchall()
        columns = result.keys()
        
        print(f"\n{table_name.upper()} Table:")
        print("-" * 80)
        print(" | ".join(f"{c:<20}" for c in columns))
        print("-" * 80)
        for row in rows:
            print(" | ".join(f"{str(v)[:20]:<20}" for v in row))


def main():
    """Run the database anonymization example."""
    # Create a temporary database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "example.db"
        connection_string = f"sqlite:///{db_path}"
        
        # Setup database with sample data
        setup_database(str(db_path))
        
        # Create engine for display purposes
        engine = create_engine(connection_string)
        
        # Display original data
        print("\n" + "=" * 80)
        print("ORIGINAL DATABASE CONTENT")
        print("=" * 80)
        display_table(engine, "users")
        display_table(engine, "orders")
        
        # Configure anonymization
        print("\n" + "=" * 80)
        print("ANONYMIZING DATABASE")
        print("=" * 80)
        
        # Configuration for each table
        config = {
            "users": {
                # Replace with realistic fake data
                "name": {"strategy": "replace", "type": "name"},
                "address": {"strategy": "replace", "type": "address"},
                "company": {"strategy": "replace", "type": "company"},
                
                # Mask phone numbers
                "phone": {"strategy": "mask", "type": "phone", "preserve_last": 4},
                
                # Hash emails (relationships will be preserved)
                "email": {"strategy": "hash", "type": "email"},
            },
            "orders": {
                # Hash emails to maintain relationship with users
                "customer_email": {"strategy": "hash", "type": "email"},
                
                # Mask shipping addresses
                "shipping_address": {"strategy": "mask", "type": "address", "preserve_last": 10},
            }
        }
        
        # Create database anonymizer
        anonymizer = DatabaseAnonymizer(connection_string, batch_size=100)
        
        # Preview anonymization
        print("\nPreview - Users table (first 3 rows):")
        print("-" * 80)
        preview = anonymizer.preview("users", config["users"], num_rows=3)
        for item in preview:
            orig = item["original"]
            anon = item["anonymized"]
            print(f"  {orig['name'][:20]:<20} -> {anon['name'][:20]:<20}")
            print(f"  {orig['email'][:20]:<20} -> {anon['email'][:20]:<20}")
            print(f"  {orig['phone']:<20} -> {anon['phone']:<20}")
            print()
        
        # Anonymize the database
        print("\nProcessing...")
        stats = anonymizer.anonymize(config)
        
        print(f"\nAnonymization complete!")
        print(f"  Tables processed: {stats['tables_processed']}")
        print(f"  Records processed: {stats['records_processed']}")
        print(f"  Fields anonymized: {stats['fields_anonymized']}")
        
        # Display anonymized data
        print("\n" + "=" * 80)
        print("ANONYMIZED DATABASE CONTENT")
        print("=" * 80)
        display_table(engine, "users")
        display_table(engine, "orders")
        
        # Demonstrate database introspection
        print("\n" + "=" * 80)
        print("DATABASE INTROSPECTION")
        print("=" * 80)
        
        print("\nTables in database:")
        for table in anonymizer.get_tables():
            print(f"  - {table}")
        
        print("\nColumns in 'users' table:")
        for col in anonymizer.get_columns("users"):
            print(f"  - {col['name']}: {col['type']}")
        
        # Clean up
        anonymizer.close()
        
        # Show connection examples for other databases
        print("\n" + "=" * 80)
        print("CONNECTION STRING EXAMPLES")
        print("=" * 80)
        print("""
SQLite:
  sqlite:///path/to/database.db

PostgreSQL:
  postgresql://username:password@localhost:5432/database_name

MySQL:
  mysql+pymysql://username:password@localhost:3306/database_name

SQL Server:
  mssql+pyodbc://username:password@dsn_name

Oracle:
  oracle+cx_oracle://username:password@host:port/database
        """)


def example_with_context_manager():
    """Example using DatabaseAnonymizer as a context manager."""
    print("\n" + "=" * 80)
    print("CONTEXT MANAGER EXAMPLE")
    print("=" * 80)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "context_example.db"
        setup_database(str(db_path))
        
        # Using context manager ensures proper cleanup
        with DatabaseAnonymizer(f"sqlite:///{db_path}") as anonymizer:
            config = {
                "users": {
                    "name": {"strategy": "replace", "type": "name"},
                    "email": {"strategy": "hash", "type": "email"},
                }
            }
            
            stats = anonymizer.anonymize(config)
            print(f"Anonymized {stats['records_processed']} records")
        # Connection automatically closed here


def example_selective_anonymization():
    """Example showing selective table anonymization."""
    print("\n" + "=" * 80)
    print("SELECTIVE TABLE ANONYMIZATION EXAMPLE")
    print("=" * 80)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "selective_example.db"
        setup_database(str(db_path))
        
        connection_string = f"sqlite:///{db_path}"
        
        with DatabaseAnonymizer(connection_string) as anonymizer:
            # Only anonymize specific columns in users table
            users_config = {
                "users": {
                    "name": {"strategy": "replace", "type": "name"},
                    # email, phone, address, company left unchanged
                }
            }
            
            # Anonymize only users table
            stats = anonymizer.anonymize(users_config, tables=["users"])
            print(f"Anonymized only 'users' table: {stats['records_processed']} records")


if __name__ == "__main__":
    main()
    example_with_context_manager()
    example_selective_anonymization()
