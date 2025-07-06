Tesla ESG Sentiment Analyzer
============================

🚀 Project Overview
-------------------

The **Tesla ESG Sentiment Analyzer** is an end-to-end data science project designed to extract, analyze, and visualize sentiment from Tesla's ESG (Environmental, Social, and Governance) related news data sourced from GDELT. This project demonstrates a comprehensive workflow, including data loading, cleaning, sentiment extraction, exploratory data analysis (EDA), SQLite database integration, SQL querying, and data export for Business Intelligence (BI) tools.

This analyzer is built for modularity and reusability, making it suitable for production environments and serving as a robust portfolio piece.

✨ Features
----------

*   **Data Ingestion**: Loads Tesla ESG news data from a CSV file, with robust handling for missing files by generating sample data.
    
*   **Data Preprocessing**:
    
    *   Parses and standardizes date formats.
        
    *   Extracts numerical sentiment scores from GDELT's V2Tone field.
        
    *   Cleans data by removing null sentiment scores and dates.
        
    *   Enriches data with temporal features (year, month, day of week) and sentiment categories.
        
*   **Sentiment Analysis**: Quantifies news sentiment towards Tesla's ESG aspects.
    
*   **Exploratory Data Analysis (EDA)**:
    
    *   Calculates and visualizes daily average sentiment trends.
        
    *   Analyzes sentiment score distribution.
        
    *   Identifies daily article counts.
        
    *   Categorizes and visualizes overall sentiment distribution.
        
    *   Highlights the top 10 most negative sentiment articles for critical review.
        
    *   Extracts and visualizes the most common ESG themes from news coverage.
        
*   **Database Integration**: Stores cleaned and processed data into an SQLite database for persistent storage and efficient querying.
    
*   **SQL Querying**: Executes advanced SQL queries to derive insights such as daily average sentiment, sentiment category breakdowns, top news sources, and monthly sentiment trends directly from the database.
    
*   **BI Tool Export**: Prepares and exports data with additional features (e.g., rolling averages) into CSV format, ready for consumption by BI tools like Power BI or Tableau.
    
*   **Robust Logging**: Implements detailed logging for all key operations, aiding in debugging and monitoring.
    

🛠️ Technologies Used
---------------------

*   **Python**: The core programming language.
    
*   **Pandas**: For data manipulation and analysis.
    
*   **NumPy**: For numerical operations.
    
*   **Matplotlib & Seaborn**: For static data visualization.
    
*   **Plotly Express & Plotly Graph Objects**: For interactive data visualization.
    
*   **SQLite3**: For local database storage and SQL querying.
    
*   **re (Regular Expressions)**: For pattern matching in text.
    
*   **logging**: For structured logging throughout the application.
    
*   **datetime, timedelta, collections.Counter**: Standard Python libraries for date/time operations and data counting.
    

📂 Project Structure
--------------------

```plaintext
Tesla-ESG-Sentiment-Analyzer/
├── data/
│   └── tesla_esg.csv                 # Raw GDELT Tesla ESG data
├── artifacts/
│   └── plots/
│       ├── sentiment_dashboard.png   # Generated sentiment visualization
│       └── esg_theme_analysis.png    # Generated ESG theme visualization
├── tesla_esg.db                      # SQLite database (generated upon run)
├── tesla_esg_cleaned_for_bi.csv      # Exported data for BI tools (generated upon run)
├── tesla_esg_summary_stats           # Exported summary statistics
├── filter_tesla_data.py              # Pre-filtering raw GDELT files for TESLA mentions
├── tesla_esg_analysis.py             # Main script to run the analysis
├── README.md                         # Project README file
├── findings.md                       # Project findings and key insights
└── requirements.txt                  # Python dependencies
```
⚙️ Setup and Installation
-------------------------

To get this project up and running on your local machine, follow these steps:

### 1\. Clone the Repository

```bash
git clone [https://github.com/your-username/Tesla-ESG-Sentiment-Analyzer.git](https://github.com/ashlynmaria/tesla_esg_sentiment.git)
cd tesla_esg_sentiment
```
### 2\. Create a Virtual Environment (Recommended)

```bash
python -m venv venv
# On Windows
.\venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate
```

### 3\. Install Dependencies

```bash
pip install -r requirements.txt
```
**requirements.txt content:**

```bash
pandas
numpy
matplotlib
seaborn
plotly
```

### 4\. Data Preparation

### Filtering Tesla-Related Data from GDELT

If you have a raw GDELT `.gkg.csv` file, you can pre-filter it for Tesla mentions using the provided `filter_tesla_data.py` script:

```bash
python filter_tesla_data.py
```
This script:

Selects the most relevant columns from GDELT Global Knowledge Graph files.

Filters rows mentioning “Tesla” in either V2Themes or Organizations.

Saves the filtered output as data/tesla_esg.csv for further processing.

After filtering, continue with the standard data preparation steps as described above.

Place your tesla\_esg.csv file inside the data/ directory. If the file is not found, the script will automatically generate sample data for demonstration purposes.

🏃 How to Run
-------------

After setting up the environment and data, you can run the sentiment analyzer by executing the main.py script.

1.  **Ensure your Python script is named tesla_esg_analysis.py** (or adjust the command below).
    
2.  python tesla_esg_analysis.py
    

The script will perform the following actions:

*   Load the data (or create sample data if tesla\_esg.csv is not found).
    
*   Clean and preprocess the data.
    
*   Perform EDA, generating plots that will be saved in artifacts/plots/.
    
*   Store the cleaned data and daily sentiment summaries in tesla\_esg.db.
    
*   Execute and display results of several SQL queries.
    
*   Export enhanced data to tesla\_esg\_cleaned\_for\_bi.csv for BI tools.
    

📈 Analysis and Visualizations
------------------------------

The project generates several key visualizations and insights during the EDA phase:

### Tesla ESG Sentiment Analysis Dashboard

A comprehensive dashboard showing:

*   **Daily Average Sentiment Over Time**: Track the trend of sentiment scores.
    
*   **Sentiment Score Distribution**: Understand the overall spread of sentiment.
    
*   **Daily Article Count**: Monitor the volume of news coverage.
    
*   **Sentiment Distribution by Category**: Breakdown of articles into 'Very Negative', 'Negative', 'Positive', and 'Very Positive' categories.
    

_(These plots are saved as sentiment\_dashboard.png in the artifacts/plots/ directory.)_

### ESG Theme Analysis

Visualizations detailing:

*   **Top 10 Most Common Themes**: Identify prevailing topics in the news data.
    
*   **ESG Theme Distribution**: A pie chart showing the proportion of news articles categorized under Environmental, Social, Governance, Economic, Technology, and Political themes.
    

_(These plots are saved as esg\_theme\_analysis.png in the artifacts/plots/ directory.)_

📊 Database and BI Export
-------------------------

The project leverages SQLite for persistent data storage, allowing for efficient querying.

The store\_to\_database() method populates two tables:

*   tesla\_esg: Contains the cleaned, article-level data.
    
*   daily\_sentiment: Stores daily aggregated sentiment metrics.
    

Additionally, the export\_data\_for\_bi() method generates tesla\_esg\_cleaned\_for\_bi.csv, which includes:

*   Original cleaned data.
    
*   Calculated fields like absolute sentiment, positive/negative flags, week/quarter.
    
*   **7-day and 30-day rolling average sentiment scores**, crucial for trend analysis in BI tools.
    
*   A separate summary statistics CSV providing an overview of key metrics.
    

💡 Future Enhancements
----------------------

*   **Advanced NLP Models**: Implement more sophisticated pre-trained NLP models (e.g., BERT, RoBERTa) for more nuanced sentiment analysis, potentially fine-tuned on financial/ESG text.
    
*   **Real-time Data Ingestion**: Integrate with GDELT's real-time API or other news APIs for continuous data updates.
    
*   **Interactive Web Dashboard**: Build a web-based dashboard using Streamlit, Dash, or Flask to visualize results dynamically without requiring local Python execution for end-users.
    
*   **Topic Modeling**: Apply topic modeling (e.g., LDA, NMF) to discover latent themes beyond predefined ESG categories.
    
*   **Anomaly Detection**: Implement algorithms to detect unusual spikes or drops in sentiment that might indicate significant events.
    
*   **Configuration File**: Externalize configuration parameters (e.g., data\_path, db\_name) into a config.ini or settings.py file.
    

📈 Findings
----------

For a detailed discussion of **Tesla ESG sentiment findings**, please see [findings.md](./findings.md).

🤝 Contributing
---------------

Contributions are welcome! If you have suggestions for improvements or new features, please feel free to open an issue or submit a pull request.

📄 License
----------

This project is open-sourced under the MIT License. See the LICENSE file for more details.

📧 Contact
----------

For any questions or inquiries, please get in touch with [Ashlyn Benoy on LinkedIn](https://www.linkedin.com/in/ashlyn-benoy-891492275).
