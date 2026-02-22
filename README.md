# inno-tech-challenge
## Summary 
This is the Repository for my solution implementation for the InnoGames Tech Challenge for the position of Senior Database DevOps Engineer

## Questionaire
### Background:
As a Data Engineer, you frequently encounter requests to retrieve data from external sources like API
endpoints or cloud storage and ingest it into your storage for further analysis. This task is designed to
evaluate your capability in implementing a basic end-to-end data pipeline. There is no single correct
solution; candidates are encouraged to use any programming language and framework of their choice.

### Task Description:
Choose either *CASE A* or CASE B; implementation of both cases is not required. _*Candidate Note*: Will go for Case A_
Please answer the following questions based on the data from the provided API. Design and implement
an end-to-end pipeline to accomplish this. The implementation should encompass an ETL (Extract,
Transform, Load) workflow for fetching raw data from API endpoints, processing and extracting
relevant information, and loading the data into tables in a local database. Candidates are free to utilize
any programming language and framework.
For sharing the result, create a GitHub repository for the tasks and share it with us when you’re ready.
Ensure that the implementation can be executed error-free, at least in your development environment.
If it’s possible, deployment of the implementation as a service would be beneficial.

### Result:
In addition to answering the questions, you must generate:

At least *three* different types of visualizations based on aggregated data.

#### Requirements:
• Visualizations can be produced using code (e.g., Python: matplotlib, seaborn, Plotly,
dashboard frameworks) or via enterprise BI tools such as PowerBI or Looker.
• Charts must be based on processed data (not raw API dumps)
• Results must be reproducible
• Clear labels, titles, and ordering are expected

### CASE A: Open Brewery Data API
#### API Documentation: Open Brewery Data API Documentation
1. Which state in the USA has the most microbreweries?
2. What are the top 5 states in the USA with the most microbreweries?
3. How many brewpub breweries are there in the state or province of Incheon, South Korea?
4. (Bonus + Optional) How would you analyze the phone number patterns with cities in Korea
based on the information given by the API?

_*Candidate Note*: As I will go with Case A, I won't be adding Case B questions here to avoid confusion._

### Challenge Resolution
As resolution for the challenge I've decided to go for Case A. The framework chosen was the following:
- For the extraction, Python to handle the API calls, normalization of the data and persisting the data into a SQL Lite database
- As for the transform, SQL will be used to clean up, aggregate and model the data into a dimensional model
- Will handle visualization of the aggregated data with a PowerBI dashboard for interesting visuals (after exporting the data to CSV), and a Notebook (Jupyter) for analyzing and answering specific questions within the data
- Making everything as a service using GitHub Actions. 
*BEWARE* the PowerBI dashboard is not possible to be refreshed using this framework as it is not possible within GitHub Actions, so this part has to be done manually in case it is necessary to refresh using the newly generated CSVs
