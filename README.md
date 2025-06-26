# ELT Orchestration: Airflow + NiFi + S3

This project demonstrates how to use Apache Airflow as an orchestrator for an Apache NiFi data pipeline. The goal is to extract data from various sources, load it into an S3 data lake, and explore the benefits and trade-offs of this integrated approach.

## The Big Picture

The core idea is to separate the 'what and when' from the 'how':

-   **Airflow (The Conductor):** Manages the schedule and workflow logic. It tells the NiFi pipeline when to start, monitors its progress, and tells it when to stop. It does not handle the data itself.
-   **NiFi (The Data Mover):** Executes the actual ELT process. It provides the visual, flow-based environment to pull data from sources (like PostgreSQL or MySQL) and push it to its final destination in an S3 data lake.
-   **Docker (The Environment):** Contains and links all services (Airflow, NiFi, databases) for a consistent, one-command setup.

## How it Works

The Airflow DAG (`s3_elt_orchestrator.py`) performs the following sequence:

1.  **Get Token:** Authenticates with the NiFi API by retrieving an access token.
2.  **Start Flow:** Triggers a specific NiFi Process Group to begin moving data.
3.  **Wait for Completion:** Polls NiFi until the data processing queue is empty.
4.  **Stop Flow:** Stops the NiFi Process Group to conserve resources.

## Key Takeaway: Why Use Both?

This pattern allows you to leverage the best of both tools, but it comes with trade-offs.

### Pros:

-   **Advanced Orchestration:** Use Airflow for complex scheduling, dependency management (e.g., "run NiFi flow only after another API task succeeds"), and robust error handling that NiFi alone may not handle as easily.
-   **Centralized Control:** Manage multiple NiFi flows and other non-NiFi tasks from a single Airflow UI, providing a unified view of all your data pipelines.

### Cons:

-   **Increased Complexity:** You now have two systems to manage, monitor, and debug. A failure requires checking both Airflow logs for orchestration errors and NiFi's UI for data flow errors.
-   **Overhead:** For simple, time-based schedules, NiFi's built-in schedulers are sufficient and adding Airflow can be unnecessary overhead.
