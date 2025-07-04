import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import warnings
import re
import logging
from datetime import datetime, timedelta
from collections import Counter
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set display options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
warnings.filterwarnings('ignore')

class TeslaESGSentimentAnalyzer:
    """
    A class to perform end-to-end sentiment analysis on Tesla ESG news data from GDELT.

    This class handles data loading, cleaning, sentiment extraction, exploratory data analysis,
    database storage, SQL querying, data export for BI tools, and advanced analytics.
    It's designed for modularity and reusability, making it suitable for production environments
    and showcasing on a resume.
    """

    def __init__(self, data_path: str = 'data/tesla_esg.csv', db_name: str = 'tesla_esg.db'):
        """
        Initializes the TeslaESGSentimentAnalyzer with data path and database name.

        Args:
            data_path (str): The file path to the Tesla ESG CSV dataset.
            db_name (str): The name of the SQLite database to use for storage.
        """
        self.data_path = data_path
        self.db_name = db_name
        self.df: pd.DataFrame = pd.DataFrame()
        self.df_clean: pd.DataFrame = pd.DataFrame()
        self.daily_sentiment: pd.DataFrame = pd.DataFrame()
        self.conn: sqlite3.Connection | None = None
        logging.info("TeslaESG sentimentAnalyzer initialized.")

    def _load_data(self) -> None:
        """
        Loads the Tesla ESG dataset from the specified CSV path.
        If the file is not found, it creates a sample DataFrame with a predefined structure.
        """
        logging.info(f"Attempting to load dataset from {self.data_path}")
        try:
            self.df = pd.read_csv(self.data_path)
            logging.info(f"Dataset loaded successfully! Shape: {self.df.shape}")
            logging.info(f"Columns: {list(self.df.columns)}")
        except FileNotFoundError:
            logging.warning(f"Error: {self.data_path} not found. Creating sample data.")
            # Sample data mimicking the actual structure from the notebook
            sample_data = {
                'SQLDATE': [1, 1, 1, 1, 1, 1, 1],
                'V2Themes': [
                    'LEGISLATION;EPU_POLICY;EPU_POLICY_LAW;WB_845_LEGAL_AND_REGULATORY_FRAMEWORK;WB_696_PUBLIC_SECTOR_MANAGEMENT;WB_969_CAPITAL_MARKETS_LAW_AND_REGULATION;WB_853_FINANCIAL_LAWS_AND_REGULATIONS;TRIAL;TAX_FNCACT;TAX_FNCACT_EXECUTIVES;APPOINTMENT;CORRUPTION;WB_2019_ANTI_CORRUPTION_LEGISLATION;WB_831_GOVERNANCE;WB_832_ANTI_CORRUPTION;WB_2020_BRIBERY_FRAUD_AND_COLLUSION;WB_678_DIGITAL_GOVERNMENT;WB_694_BROADCAST_AND_MEDIA;WB_133_INFORMATION_AND_COMMUNICATION_TECHNOLOGIES;MEDIA_SOCIAL;WB_652_ICT_APPLICATIONS;WB_662_SOCIAL_MEDIA;WB_658_ENTERPRISE_APPLICATIONS;TRAFFIC;ECON_STOCKMARKET;TAX_ECON_PRICE;TAX_FNCACT_ANALYST;WB_962_INTERNATIONAL_LAW;TAX_FNCACT_ATTORNEYS;TAX_FNCACT_DIRECTORS;WB_1331_HEALTH_TECHNOLOGIES;WB_1350_PHARMACEUTICALS;WB_621_HEALTH_NUTRITION_AND_POPULATION;TAX_FNCACT_ATTORNEY',
                    'LEGISLATION;EPU_POLICY;EPU_POLICY_LAW;WB_845_LEGAL_AND_REGULATORY_FRAMEWORK;WB_696_PUBLIC_SECTOR_MANAGEMENT;WB_969_CAPITAL_MARKETS_LAW_AND_REGULATION;WB_853_FINANCIAL_LAWS_AND_REGULATIONS;TRIAL;TAX_FNCACT;TAX_FNCACT_EXECUTIVES;APPOINTMENT;CORRUPTION;WB_2019_ANTI_CORRUPTION_LEGISLATION;WB_831_GOVERNANCE;WB_832_ANTI_CORRUPTION;WB_2020_BRIBERY_FRAUD_AND_COLLUSION;TAX_FNCACT_ASSISTANT;SOC_EMERGINGTECH;ECON_STOCKMARKET;DELAY;USPEC_UNCERTAINTY1;TAX_ECON_PRICE;WB_698_TRADE;TAX_FNCACT_DEVELOPER;WB_678_DIGITAL_GOVERNMENT;WB_694_BROADCAST_AND_MEDIA;WB_133_INFORMATION_AND_COMMUNICATION_TECHNOLOGIES;WB_962_INTERNATIONAL_LAW;TAX_FNCACT_ATTORNEYS;TAX_FNCACT_DIRECTORS;WB_1331_HEALTH_TECHNOLOGIES;WB_1350_PHARMACEUTICALS;WB_621_HEALTH_NUTRITION_AND_POPULATION;TAX_FNCACT_ATTORNEY',
                    'ECON_STOCKMARKET;TAX_ETHNICITY;TAX_ETHNICITY_CHINESE;TAX_WORLDLANGUAGES;TAX_WORLDLANGUAGES_CHINESE',
                    'ECON_STOCKMARKET;LEADER;TAX_FNCACT;TAX_FNCACT_PRESIDENT;USPEC_POLITICS_GENERAL1;TAX_ECON_PRICE;TAX_FNCACT_PERFORMER;TAX_FNCACT_MANUFACTURER;WB_698_TRADE;NATURAL_DISASTER;NATURAL_DISASTER_FLOOD',
                    'TAX_POLITICAL_PARTY;TAX_POLITICAL_PARTY_LIBERTARIAN;USPEC_POLITICS_GENERAL1;TAX_FNCACT;TAX_FNCACT_CEO;TAX_POLITICAL_PARTY_LIBERTARIAN_PARTY;TAX_FNCACT_POLITICO;TAX_DISEASE;TAX_DISEASE_DISRUPTIVE;ELECTION;USPEC_POLICY1;EPU_POLICY;EPU_POLICY_BUDGET;TAX_POLITICAL_PARTY_REPUBLICAN;EPU_POLICY_POLITICAL;WB_926_POLITICAL_PARTICIPATION;WB_615_GENDER;WB_924_VOICE_AND_AGENCY;LEADER;TAX_FNCACT_PRESIDENT;EPU_POLICY_SPENDING;LEGISLATION;UNGP_FORESTS_RIVERS_OCEANS;ARMEDCONFLICT;TAX_FNCACT_LEADER',
                    'LEGISLATION;EPU_POLICY;EPU_POLICY_LAW;WB_845_LEGAL_AND_REGULATORY_FRAMEWORK;WB_696_PUBLIC_SECTOR_MANAGEMENT;WB_969_CAPITAL_MARKETS_LAW_AND_REGULATION;WB_853_FINANCIAL_LAWS_AND_REGULATIONS;TRIAL;GENERAL_HEALTH;MEDICAL;TAX_FNCACT;TAX_FNCACT_EXECUTIVES;APPOINTMENT;CORRUPTION;WB_2019_ANTI_CORRUPTION_LEGISLATION;WB_831_GOVERNANCE;WB_832_ANTI_CORRUPTION;WB_2020_BRIBERY_FRAUD_AND_COLLUSION;TAX_DISEASE;TAX_DISEASE_WEIGHT_LOSS;WB_1331_HEALTH_TECHNOLOGIES;WB_2453_ORGANIZED_CRIME;WB_1350_PHARMACEUTICALS;WB_2433_CONFLICT_AND_VIOLENCE;WB_621_HEALTH_NUTRITION_AND_POPULATION;WB_2432_FRAGILITY_CONFLICT_AND_VIOLENCE;WB_2456_DRUGS_AND_NARCOTICS;UNGP_FORESTS_RIVERS_OCEANS;EPU_CATS_HEALTHCARE;USPEC_POLICY1;ECON_STOCKMARKET;CRISISLEX_C03_WELLBEING_HEALTH;UNGP_CRIME_VIOLENCE;BAN;TAX_ECON_PRICE;WB_962_INTERNATIONAL_LAW;TAX_FNCACT_ATTORNEYS;TAX_FNCACT_DIRECTORS;TAX_FNCACT_ATTORNEY',
                    'LEGISLATION;EPU_POLICY;EPU_POLICY_LAW;WB_845_LEGAL_AND_REGULATORY_FRAMEWORK;WB_696_PUBLIC_SECTOR_MANAGEMENT;WB_969_CAPITAL_MARKETS_LAW_AND_REGULATION;WB_853_FINANCIAL_LAWS_AND_REGULATIONS;TRIAL;TAX_FNCACT;TAX_FNCACT_EXECUTIVES;APPOINTMENT;CORRUPTION;WB_2019_ANTI_CORRUPTION_LEGISLATION;WB_831_GOVERNANCE;WB_832_ANTI_CORRUPTION;WB_2020_BRIBERY_FRAUD_AND_COLLUSION;TAX_FNCACT_DESIGNER;TAX_FNCACT_MANUFACTURER;TAX_ECON_PRICE;ECON_STOCKMARKET;WB_2024_ANTI_CORRUPTION_AUTHORITIES;WB_840_JUSTICE;WB_2025_INVESTIGATION;WB_1014_CRIMINAL_JUSTICE;DELAY;USPEC_UNCERTAINTY1;TAX_FNCACT_FOUNDER;RESIGNATION;WB_2048_COMPENSATION_CAREERS_AND_INCENTIVES;WB_723_PUBLIC_ADMINISTRATION;WB_724_HUMAN_RESOURCES_FOR_PUBLIC_SECTOR;WB_962_INTERNATIONAL_LAW;TAX_FNCACT_ATTORNEYS;TAX_FNCACT_DIRECTORS;WB_1331_HEALTH_TECHNOLOGIES;WB_1350_PHARMACEUTICALS;WB_621_HEALTH_NUTRITION_AND_POPULATION;TAX_FNCACT_ATTORNEY'
                ],
                'Organizations': [
                    'google;bleichmar fonti auld;news network;securities exchange;tesla inc;u s district court;reddit inc;why bleichmar fonti auld',
                    'cnn;nasdaq;bleichmar fonti auld;news network;securities exchange;u s district court;tesla inc;why bleichmar fonti auld;apple inc',
                    'tesla service center',
                    'apple inc;nvidia;goldman sachs;alphabet inc class;nasdaq;microsoft;microsoft corp;meta platforms inc;tesla inc',
                    'tesla inc;america party;libertarian party;libertarian national committee',
                    'u s district court;news network;hims hers health inc;why bleichmar fonti auld;bleichmar fonti auld;securities exchange;novo nordisk;tesla inc',
                    'bleichmar fonti auld;news network;compass group diversified holdings;compass group diversified holdings inc;compass diversified holdings;why bleichmar fonti auld;lugano holdings inc;securities exchange;tesla inc;u s district court'
                ],
                'V2Tone': [
                    '-2.43111831442464,1.45867098865478,3.88978930307942,5.3484602917342,17.5040518638574,0.972447325769854',
                    '-1.81268882175227,1.96374622356495,3.77643504531722,5.74018126888217,16.4652567975831,0.906344410876133',
                    '2.89855072463768,7.2463768115942,4.34782608695652,11.5942028985507,17.3913043478261,0',
                    '1.38648180242634,2.77296360485269,1.38648180242634,4.15944540727903,16.1178509532062,4.15944540727903',
                    '2.05655526992288,5.1413881748072,3.08483290488432,8.22622107969152,21.0796915167095,0',
                    '-1.39751552795031,2.79503105590062,4.19254658385093,6.98757763975155,15.6832298136646,0.93167701863354',
                    '-2.1558872305141,1.32669983416252,3.48258706467662,4.80928689883914,20.7296849087894,1.16086235489221'
                ],
                'SourceCollectionIdentifier': [
                    'pr-inside.com', 'pr-inside.com', 'seekingalpha.com', 'fool.com.au',
                    'benzinga.com', 'pr-inside.com', 'pr-inside.com'
                ],
                'DocumentIdentifier': [
                    'https://www.pr-inside.com/rddt-class-action-alert-reddit-inc-shareholders-with-losses-are-r5117393.htm',
                    'https://www.pr-inside.com/aapl-class-action-alert-apple-inc-shareholders-with-losses-are-notified-r5117385.htm',
                    'https://seekingalpha.com/news/4464601-tesla-breaks-8-month-china-sales-slump',
                    'https://www.fool.com.au/2025/07/03/how-did-the-us-magnificent-seven-stocks-perform-in-fy25/',
                    'https://www.benzinga.com/news/politics/25/07/46239662/elon-musk-receives-invitation-to-join-libertarian-party-making-a-new-third-party-would-be-a-mistake',
                    'https://www.pr-inside.com/hims-class-action-alert-hims-hers-health-inc-shareholders-with-r5117390.htm',
                    'https://www.pr-inside.com/codi-class-action-alert-compass-diversified-holdings-shareholders-r5117386.htm'
                ]
            }
            self.df = pd.DataFrame(sample_data)
            logging.info(f"Sample dataset created with shape: {self.df.shape}")
        logging.info("\n📊 Dataset Overview:")
        logging.info("-" * 30)
        self.df.info()
        logging.info("\n📈 First few rows:")
        logging.info(self.df.head())

    def _extract_sentiment(self, tone_str: str | float) -> float:
        """
        Extracts the first sentiment score from the GDELT V2Tone string.

        Args:
            tone_str (str | float): The V2Tone string or a direct float value.

        Returns:
            float: The extracted sentiment score, or NaN if extraction fails.
        """
        if pd.isna(tone_str):
            return np.nan
        if isinstance(tone_str, (int, float)):
            return float(tone_str)
        try:
            # GDELT V2Tone format: "tone,positive_score,negative_score,polarity,activity_ref,self_ref"
            parts = str(tone_str).split(',')
            return float(parts[0]) if parts[0] else np.nan
        except (ValueError, IndexError):
            return np.nan

    def _extract_themes(self, theme_string: str) -> list[str]:
        """
        Extracts individual themes from the V2Themes string.

        Args:
            theme_string (str): A string containing semicolon-separated themes.

        Returns:
            list[str]: A list of cleaned theme strings.
        """
        if pd.isna(theme_string):
            return []
        return [theme.strip() for theme in str(theme_string).split(';') if theme.strip()]

    def _categorize_esg_themes(self, themes: list[str]) -> dict[str, int]:
        """
        Categorizes a list of themes into ESG categories.

        Args:
            themes (list[str]): A list of extracted themes.

        Returns:
            dict[str, int]: A dictionary with counts for each ESG category.
        """
        esg_categories_map = {
            'Environmental': ['WB_1331_HEALTH_TECHNOLOGIES', 'UNGP_FORESTS_RIVERS_OCEANS', 'NATURAL_DISASTER',
                              'TAX_ETHNICITY_CHINESE', 'TAX_WORLDLANGUAGES_CHINESE'],
            'Social': ['WB_615_GENDER', 'WB_924_VOICE_AND_AGENCY', 'WB_621_HEALTH_NUTRITION_AND_POPULATION',
                       'GENERAL_HEALTH', 'MEDICAL', 'TAX_DISEASE', 'WB_926_POLITICAL_PARTICIPATION'],
            'Governance': ['WB_831_GOVERNANCE', 'WB_832_ANTI_CORRUPTION', 'WB_2019_ANTI_CORRUPTION_LEGISLATION',
                           'WB_2020_BRIBERY_FRAUD_AND_COLLUSION', 'CORRUPTION', 'LEGISLATION', 'EPU_POLICY',
                           'WB_845_LEGAL_AND_REGULATORY_FRAMEWORK', 'WB_696_PUBLIC_SECTOR_MANAGEMENT',
                           'WB_969_CAPITAL_MARKETS_LAW_AND_REGULATION', 'TAX_FNCACT_EXECUTIVES'],
            'Economic': ['ECON_STOCKMARKET', 'TAX_ECON_PRICE', 'WB_698_TRADE'],
            'Technology': ['WB_678_DIGITAL_GOVERNMENT', 'WB_694_BROADCAST_AND_MEDIA',
                           'WB_133_INFORMATION_AND_COMMUNICATION_TECHNOLOGIES', 'SOC_EMERGINGTECH',
                           'WB_652_ICT_APPLICATIONS', 'WB_662_SOCIAL_MEDIA'],
            'Political': ['USPEC_POLITICS_GENERAL1', 'TAX_POLITICAL_PARTY', 'ELECTION', 'TAX_FNCACT_PRESIDENT']
        }

        categorized_counts = {cat: 0 for cat in esg_categories_map.keys()}

        for theme in themes:
            for category, keywords in esg_categories_map.items():
                if any(keyword in theme for keyword in keywords):
                    categorized_counts[category] += 1
                    break
        return categorized_counts

    def clean_and_preprocess_data(self) -> None:
        """
        Performs data cleaning and preprocessing steps:
        - Parses the date column.
        - Extracts sentiment scores.
        - Drops rows with null sentiment scores or dates.
        - Creates additional temporal and sentiment category columns.
        """
        logging.info("\n🧹 Starting Data Cleaning Process...")
        logging.info("=" * 40)

        original_size = len(self.df)
        logging.info(f"Original dataset size: {original_size:,} records")

        logging.info("\n📅 Parsing date column...")
        try:
            if 'SQLDATE' in self.df.columns and self.df['SQLDATE'].dtype == 'int64' and len(str(self.df['SQLDATE'].iloc[0])) == 8:
                self.df['date'] = pd.to_datetime(self.df['SQLDATE'], format='%Y%m%d', errors='coerce')
            else:
                logging.warning("SQLDATE appears to be placeholder data or not in YYYYMMDD format. Creating sample dates...")
                base_date = pd.to_datetime('2024-07-01')
                self.df['date'] = [base_date + pd.Timedelta(days=i) for i in range(len(self.df))]
            logging.info("Date column parsed successfully.")
        except Exception as e:
            logging.error(f"Error parsing date column: {e}. Falling back to sequential dates.")
            base_date = pd.to_datetime('2024-07-01')
            self.df['date'] = [base_date + pd.Timedelta(days=i) for i in range(len(self.df))]
            logging.info("Date column created with fallback method.")

        logging.info("\n🎯 Processing sentiment scores...")
        self.df['sentiment_score'] = self.df['V2Tone'].apply(self._extract_sentiment)
        logging.info(f"Sentiment scores extracted. Range: {self.df['sentiment_score'].min():.2f} to {self.df['sentiment_score'].max():.2f}")

        logging.info("\n🗑️ Removing null values...")
        logging.info(f"Null values before cleaning:\n{self.df.isnull().sum()}")
        self.df_clean = self.df.dropna(subset=['sentiment_score', 'date']).copy()
        logging.info(f"Removed {original_size - len(self.df_clean):,} rows with null values.")
        logging.info(f"Final dataset size: {len(self.df_clean):,} records.")

        logging.info("\n🔧 Creating additional columns...")
        self.df_clean['year'] = self.df_clean['date'].dt.year
        self.df_clean['month'] = self.df_clean['date'].dt.month
        self.df_clean['day_of_week'] = self.df_clean['date'].dt.day_name()
        self.df_clean['sentiment_category'] = pd.cut(self.df_clean['sentiment_score'],
                                                     bins=[-float('inf'), -2, 0, 2, float('inf')],
                                                     labels=['Very Negative', 'Negative', 'Positive', 'Very Positive'],
                                                     right=True) # Ensure correct interval handling
        logging.info("Additional columns created: year, month, day_of_week, sentiment_category.")

    def perform_eda(self) -> None:
        """
        Executes exploratory data analysis (EDA) steps:
        - Calculates daily average sentiment.
        - Plots sentiment time series, distribution, article count, and sentiment category distribution.
        - Identifies top 10 most negative sentiment articles.
        - Analyzes and visualizes most common ESG themes and their categorization.
        """
        logging.info("\n📊 Starting Exploratory Data Analysis...")
        logging.info("=" * 45)

        logging.info("\n📈 Calculating daily average sentiment...")
        self.daily_sentiment = self.df_clean.groupby('date').agg(
            sentiment_score=('sentiment_score', 'mean'),
            article_count=('DocumentIdentifier', 'count'),
            sentiment_std=('sentiment_score', 'std')
        ).round(3).reset_index()
        self.daily_sentiment.columns = ['date', 'avg_sentiment', 'article_count', 'sentiment_std']

        logging.info(f"Daily sentiment calculated for {len(self.daily_sentiment)} days.")
        logging.info(f"Date range: {self.daily_sentiment['date'].min()} to {self.daily_sentiment['date'].max()}")

        logging.info("\n📊 Creating sentiment time series visualization...")
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Tesla ESG Sentiment Analysis Dashboard', fontsize=16, fontweight='bold')

        # Plot 1: Daily average sentiment over time
        axes[0, 0].plot(self.daily_sentiment['date'], self.daily_sentiment['avg_sentiment'],
                         color='steelblue', linewidth=2, alpha=0.8)
        axes[0, 0].axhline(y=0, color='red', linestyle='--', alpha=0.5)
        axes[0, 0].set_title('Daily Average Sentiment Over Time')
        axes[0, 0].set_xlabel('Date')
        axes[0, 0].set_ylabel('Average Sentiment Score')
        axes[0, 0].grid(True, alpha=0.3)
        axes[0, 0].tick_params(axis='x', rotation=45)

        # Plot 2: Sentiment distribution
        axes[0, 1].hist(self.df_clean['sentiment_score'], bins=50, alpha=0.7, color='lightcoral', edgecolor='black')
        axes[0, 1].axvline(x=0, color='red', linestyle='--', alpha=0.7)
        axes[0, 1].set_title('Sentiment Score Distribution')
        axes[0, 1].set_xlabel('Sentiment Score')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].grid(True, alpha=0.3)

        # Plot 3: Daily article count
        axes[1, 0].bar(self.daily_sentiment['date'], self.daily_sentiment['article_count'],
                         alpha=0.7, color='lightgreen', width=1)
        axes[1, 0].set_title('Daily Article Count')
        axes[1, 0].set_xlabel('Date')
        axes[1, 0].set_ylabel('Number of Articles')
        axes[1, 0].tick_params(axis='x', rotation=45)

        # Plot 4: Sentiment by category
        sentiment_counts = self.df_clean['sentiment_category'].value_counts().reindex(['Very Negative', 'Negative', 'Positive', 'Very Positive'])
        axes[1, 1].pie(sentiment_counts.values, labels=sentiment_counts.index, autopct='%1.1f%%',
                        colors=['#DC143C', '#FF8C00', '#90EE90', '#228B22']) # Adjusted colors for better visual
        axes[1, 1].set_title('Sentiment Distribution by Category')

        plt.tight_layout()
        plt.show()
        fig.savefig("artifacts/plots/sentiment_dashboard.png", bbox_inches="tight", dpi=300)


        logging.info("\n🔍 Identifying top 10 most negative sentiment articles...")
        negative_articles = self.df_clean.nsmallest(10, 'sentiment_score')[
            ['date', 'sentiment_score', 'DocumentIdentifier', 'SourceCollectionIdentifier']
        ].reset_index(drop=True)

        logging.info("Top 10 Most Negative Sentiment Articles:")
        logging.info("-" * 50)
        for idx, row in negative_articles.iterrows():
            logging.info(f"{idx+1}. Sentiment: {row['sentiment_score']:.2f} | Date: {row['date'].strftime('%Y-%m-%d')}")
            logging.info(f"    Source: {row['SourceCollectionIdentifier']}")
            logging.info(f"    URL: {row['DocumentIdentifier']}\n")

        logging.info("\n🏷️ Analyzing ESG themes...")
        all_themes = []
        theme_categories_list = []

        for themes_str in self.df_clean['V2Themes']:
            theme_list = self._extract_themes(themes_str)
            all_themes.extend(theme_list)
            theme_categories_list.append(self._categorize_esg_themes(theme_list))

        theme_counts = Counter(all_themes)
        top_themes = theme_counts.most_common(15)

        logging.info("Top 15 Most Common Themes in Tesla Coverage:")
        logging.info("-" * 50)
        for i, (theme, count) in enumerate(top_themes, 1):
            logging.info(f"{i:2d}. {theme}: {count:,} occurrences")

        logging.info("\n🏢 ESG Category Breakdown:")
        logging.info("-" * 30)
        category_totals = {cat: 0 for cat in ['Environmental', 'Social', 'Governance', 'Economic', 'Technology', 'Political']}

        for categories in theme_categories_list:
            for category, count in categories.items():
                category_totals[category] += count

        total_categorized = sum(category_totals.values())
        logging.info(f"Total categorized themes: {total_categorized}")

        for category, count in sorted(category_totals.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_categorized * 100) if total_categorized > 0 else 0
            logging.info(f"{category}: {count:,} ({percentage:.1f}%)")

        # Visualize ESG categories
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Top themes bar chart
        themes_df = pd.DataFrame(top_themes[:10], columns=['Theme', 'Count'])
        ax1.barh(range(len(themes_df)), themes_df['Count'], color='skyblue')
        ax1.set_yticks(range(len(themes_df)))
        ax1.set_yticklabels([theme[:30] + '...' if len(theme) > 30 else theme for theme in themes_df['Theme']])
        ax1.set_xlabel('Number of Occurrences')
        ax1.set_title('Top 10 Most Common Themes')
        ax1.invert_yaxis()  # Put the highest count at the top
        ax1.grid(axis='x', alpha=0.3)

        # ESG categories pie chart
        categories_pie = list(category_totals.keys())
        values_pie = list(category_totals.values())
        colors = ['#2E8B57', '#4682B4', '#DAA520', '#DC143C', '#9932CC', '#FF8C00']

        ax2.pie(values_pie, labels=categories_pie, autopct='%1.1f%%', colors=colors, startangle=90)
        ax2.set_title('ESG Theme Distribution')

        plt.tight_layout()
        plt.show()
        fig.savefig("artifacts/plots/esg_theme_analysis.png", bbox_inches="tight", dpi=300)


    def store_to_database(self) -> None:
        """
        Connects to a SQLite database and stores the cleaned DataFrame and daily sentiment
        summary into separate tables.
        """
        logging.info("\n💾 Storing cleaned data in SQLite database...")
        logging.info("=" * 45)

        try:
            self.conn = sqlite3.connect(self.db_name)
            self.df_clean.to_sql('tesla_esg', self.conn, if_exists='replace', index=False)
            self.daily_sentiment.to_sql('daily_sentiment', self.conn, if_exists='replace', index=False)
            logging.info(f"Data successfully stored in {self.db_name}")
            logging.info(f" - tesla_esg table: {len(self.df_clean):,} records")
            logging.info(f" - daily_sentiment table: {len(self.daily_sentiment):,} records")

            test_query = "SELECT COUNT(*) FROM tesla_esg"
            result = pd.read_sql_query(test_query, self.conn)
            logging.info(f"Database verification: {result.iloc[0, 0]:,} records in tesla_esg table.")

        except sqlite3.Error as e:
            logging.error(f"Error storing data in database: {e}")
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None # Reset connection after closing

    def perform_sql_analysis(self) -> None:
        """
        Connects to the SQLite database and performs several SQL queries to analyze the data,
        including daily average sentiment, sentiment breakdown, top news sources, and monthly trends.
        """
        logging.info("\n🔍 Performing SQL Analysis...")
        logging.info("=" * 35)

        try:
            self.conn = sqlite3.connect(self.db_name, detect_types=sqlite3.PARSE_DECLTYPES)
            
            # Ensure 'date' column in sqlite is stored as TEXT in 'YYYY-MM-DD' for date functions to work reliably
            # For pandas to parse dates correctly from SQL, specify parse_dates
            
            # Query 1: Daily average sentiment
            logging.info("\n📊 Query 1: Daily Average Sentiment (Last 30 Days)")
            query1 = """
            SELECT 
                date,
                ROUND(AVG(sentiment_score), 3) as avg_sentiment,
                COUNT(*) as article_count,
                ROUND(MIN(sentiment_score), 3) as min_sentiment,
                ROUND(MAX(sentiment_score), 3) as max_sentiment
            FROM tesla_esg 
            WHERE date >= date('now', '-30 days')
            GROUP BY date 
            ORDER BY date DESC 
            LIMIT 10
            """
            daily_avg = pd.read_sql_query(query1, self.conn, parse_dates=['date'])
            logging.info(daily_avg.to_string(index=False))

            # Query 2: Count of articles with negative sentiment
            logging.info("\n📉 Query 2: Articles with Sentiment Category Breakdown")
            query2 = """
            SELECT 
                CASE 
                    WHEN sentiment_score < -2 THEN 'Negative'
                    WHEN sentiment_score <= 2 THEN 'Neutral'
                    ELSE 'Positive'
                END as sentiment_category,
                COUNT(*) as article_count,
                ROUND(AVG(sentiment_score), 3) as avg_score,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tesla_esg), 2) as percentage
            FROM tesla_esg 
            GROUP BY sentiment_category 
            ORDER BY avg_score
            """
            sentiment_breakdown = pd.read_sql_query(query2, self.conn)
            logging.info(sentiment_breakdown.to_string(index=False))

            # Query 3: Source analysis
            logging.info("\n📰 Query 3: Top News Sources by Article Count")
            query3 = """
            SELECT 
                SourceCollectionIdentifier as source,
                COUNT(*) as article_count,
                ROUND(AVG(sentiment_score), 3) as avg_sentiment,
                ROUND(MIN(sentiment_score), 3) as min_sentiment,
                ROUND(MAX(sentiment_score), 3) as max_sentiment
            FROM tesla_esg 
            GROUP BY SourceCollectionIdentifier 
            ORDER BY article_count DESC 
            LIMIT 10
            """
            source_analysis = pd.read_sql_query(query3, self.conn)
            logging.info(source_analysis.to_string(index=False))

            # Query 4: Monthly trends
            logging.info("\n📅 Query 4: Monthly Sentiment Trends")
            query4 = """
            SELECT 
                strftime('%Y-%m', date) as month,
                COUNT(*) as article_count,
                ROUND(AVG(sentiment_score), 3) as avg_sentiment,
                ROUND(
                    SUM(CASE WHEN sentiment_score < 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
                    2
                ) as negative_percentage
            FROM tesla_esg 
            GROUP BY strftime('%Y-%m', date)
            ORDER BY month DESC
            LIMIT 12
            """
            monthly_trends = pd.read_sql_query(query4, self.conn)
            logging.info(monthly_trends.to_string(index=False))

        except sqlite3.Error as e:
            logging.error(f"Error executing SQL queries: {e}")
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

    def export_data_for_bi(self) -> None:
        """
        Prepares and exports the cleaned data into CSV files suitable for Business Intelligence tools
        like Power BI or Tableau. This includes adding rolling averages and summary statistics.
        """
        logging.info("\n📤 Exporting data for Power BI/Tableau...")
        logging.info("=" * 45)

        try:
            export_df = self.df_clean.copy()
            
            export_df['sentiment_abs'] = abs(export_df['sentiment_score'])
            export_df['is_negative'] = export_df['sentiment_score'] < 0
            export_df['is_positive'] = export_df['sentiment_score'] > 0
            export_df['week_of_year'] = export_df['date'].dt.isocalendar().week.astype(int)
            export_df['quarter'] = export_df['date'].dt.quarter
            export_df['date_str'] = export_df['date'].dt.strftime('%Y-%m-%d')
            
            export_df = export_df.sort_values('date')
            export_df['sentiment_7d_avg'] = export_df['sentiment_score'].rolling(window=7, min_periods=1).mean()
            export_df['sentiment_30d_avg'] = export_df['sentiment_score'].rolling(window=30, min_periods=1).mean()
            
            export_filename = 'tesla_esg_cleaned_for_bi.csv'
            export_df.to_csv(export_filename, index=False)
            logging.info(f"Data exported to {export_filename}")
            logging.info(f" - Records: {len(export_df):,}")
            logging.info(f" - Columns: {len(export_df.columns)}")
            logging.info(f" - Date range: {export_df['date'].min()} to {export_df['date'].max()}")
            
            # Create a summary statistics file
            summary_stats = {
                'metric': [
                    'Total Articles',
                    'Date Range Start',
                    'Date Range End',
                    'Average Sentiment',
                    'Median Sentiment',
                    'Standard Deviation',
                    'Negative Articles (%)',
                    'Positive Articles (%)',
                    'Most Common Source',
                    'Most Negative Day',
                    'Most Positive Day'
                ],
                'value': [
                    len(export_df),
                    export_df['date'].min().strftime('%Y-%m-%d'),
                    export_df['date'].max().strftime('%Y-%m-%d'),
                    round(export_df['sentiment_score'].mean(), 3),
                    round(export_df['sentiment_score'].median(), 3),
                    round(export_df['sentiment_score'].std(), 3),
                    round((export_df['sentiment_score'] < 0).sum() / len(export_df) * 100, 2),
                    round((export_df['sentiment_score'] > 0).sum() / len(export_df) * 100, 2),
                    export_df['SourceCollectionIdentifier'].value_counts().index[0] if not export_df['SourceCollectionIdentifier'].empty else 'N/A',
                    self.daily_sentiment.loc[self.daily_sentiment['avg_sentiment'].idxmin(), 'date'].strftime('%Y-%m-%d') if not self.daily_sentiment.empty else 'N/A',
                    self.daily_sentiment.loc[self.daily_sentiment['avg_sentiment'].idxmax(), 'date'].strftime('%Y-%m-%d') if not self.daily_sentiment.empty else 'N/A'
                ]
            }
            
            summary_df = pd.DataFrame(summary_stats)
            summary_df.to_csv('tesla_esg_summary_stats.csv', index=False)
            
            logging.info(f"Summary statistics exported to tesla_esg_summary_stats.csv")
            
            logging.info("\n📋 Column Information for BI Tools:")
            logging.info("-" * 40)
            column_info = pd.DataFrame({
                'Column': export_df.columns,
                'Type': export_df.dtypes,
                'Sample_Value': [str(export_df[col].iloc[0]) if len(export_df) > 0 else 'N/A' for col in export_df.columns]
            })
            logging.info(column_info.to_string(index=False))
            
        except Exception as e:
            logging.error(f"Error exporting data: {e}")

    def perform_advanced_analytics(self) -> None:
        """
        Conducts advanced analytics on the cleaned data, including:
        - Correlation analysis between numerical features.
        - Sentiment volatility analysis on a monthly basis.
        - Average sentiment by day of the week.
        - Sentiment analysis by news source.
        """
        logging.info("\n🔬 Advanced Analytics and Insights...")
        logging.info("=" * 45)

        logging.info("\n📊 Correlation Analysis:")
        available_numeric_cols = []
        potential_cols = ['sentiment_score', 'year', 'month']

        for col in potential_cols:
            if col in self.df_clean.columns:
                available_numeric_cols.append(col)

        if 'sentiment_score' in self.df_clean.columns:
            self.df_clean['sentiment_abs'] = abs(self.df_clean['sentiment_score'])
            available_numeric_cols.append('sentiment_abs')

        if len(available_numeric_cols) > 1:
            correlation_matrix = self.df_clean[available_numeric_cols].corr()
            logging.info(correlation_matrix.round(3))
        else:
            logging.warning("Not enough numeric columns for correlation analysis.")

        logging.info("\n📈 Sentiment Volatility Analysis:")
        if 'sentiment_score' in self.df_clean.columns and 'date' in self.df_clean.columns:
            monthly_volatility = self.df_clean.groupby([self.df_clean['date'].dt.to_period('M')])['sentiment_score'].std()
            logging.info(f"Average monthly volatility: {monthly_volatility.mean():.3f}")
            if len(monthly_volatility) > 0:
                logging.info(f"Highest volatility month: {monthly_volatility.idxmax()} ({monthly_volatility.max():.3f})")
                logging.info(f"Lowest volatility month: {monthly_volatility.idxmin()} ({monthly_volatility.min():.3f})")
            else:
                logging.warning("No monthly volatility data available.")
        else:
            logging.warning("Required columns not available for volatility analysis.")

        logging.info("\n📅 Day of Week Analysis:")
        if 'day_of_week' in self.df_clean.columns and 'sentiment_score' in self.df_clean.columns:
            dow_analysis = self.df_clean.groupby('day_of_week')['sentiment_score'].agg(['mean', 'count', 'std']).round(3)
            logging.info(dow_analysis)
        else:
            logging.warning("Required columns not available for day of week analysis.")

        logging.info("\n📰 Source Sentiment Analysis:")
        if 'SourceCollectionIdentifier' in self.df_clean.columns and 'sentiment_score' in self.df_clean.columns:
            source_sentiment = self.df_clean.groupby('SourceCollectionIdentifier')['sentiment_score'].agg(['mean', 'count']).round(3)
            source_sentiment = source_sentiment[source_sentiment['count'] >= 1]
            logging.info(source_sentiment.head(10))
        else:
            logging.warning("Required columns not available for source analysis.")

    def generate_executive_summary(self) -> None:
        """
        Generates a comprehensive executive summary of the Tesla ESG sentiment analysis,
        including key metrics, trend analysis, and data-driven recommendations.
        """
        logging.info("\n" + "="*60)
        logging.info("📋 TESLA ESG SENTIMENT ANALYSIS - EXECUTIVE SUMMARY")
        logging.info("="*60)

        if self.df_clean.empty:
            logging.warning("No data available for executive summary. Please run the pipeline first.")
            return

        total_articles = len(self.df_clean)
        avg_sentiment = self.df_clean['sentiment_score'].mean()
        negative_pct = (self.df_clean['sentiment_score'] < 0).sum() / total_articles * 100
        positive_pct = (self.df_clean['sentiment_score'] > 0).sum() / total_articles * 100
        
        min_date = self.df_clean['date'].min()
        max_date = self.df_clean['date'].max()
        date_range = f"{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"

        logging.info(f"\n📊 KEY METRICS:")
        logging.info(f"   • Total Articles Analyzed: {total_articles:,}")
        logging.info(f"   • Analysis Period: {date_range}")
        logging.info(f"   • Average Sentiment Score: {avg_sentiment:.3f}")
        logging.info(f"   • Negative Sentiment: {negative_pct:.1f}% of articles")
        logging.info(f"   • Positive Sentiment: {positive_pct:.1f}% of articles")

        recent_30d = self.df_clean[self.df_clean['date'] >= max_date - timedelta(days=30)]
        recent_sentiment = recent_30d['sentiment_score'].mean() if not recent_30d.empty else avg_sentiment
        
        trend_direction = "improving" if recent_sentiment > avg_sentiment else "declining"
        trend_magnitude = abs(recent_sentiment - avg_sentiment)

        logging.info(f"\n📈 TREND ANALYSIS:")
        logging.info(f"   • Recent 30-day sentiment: {recent_sentiment:.3f}")
        logging.info(f"   • Overall sentiment: {avg_sentiment:.3f}")
        logging.info(f"   • Trend: {trend_direction.upper()} by {trend_magnitude:.3f} points")

        logging.info(f"\n🎯 DATA-DRIVEN RECOMMENDATIONS FOR TESLA'S ESG STRATEGY:")
        logging.info("-" * 60)

        # Analyze the actual themes in the data from the _categorize_esg_themes method
        all_themes_list = []
        for themes_str in self.df_clean['V2Themes']:
            all_themes_list.extend(self._extract_themes(themes_str))
        
        category_totals = {cat: 0 for cat in ['Environmental', 'Social', 'Governance', 'Economic', 'Technology', 'Political']}
        for themes_str in self.df_clean['V2Themes']:
            theme_list = self._extract_themes(themes_str)
            categorized_counts = self._categorize_esg_themes(theme_list)
            for category, count in categorized_counts.items():
                category_totals[category] += count

        # Sort categories by total count to prioritize recommendations
        sorted_categories = sorted(category_totals.items(), key=lambda x: x[1], reverse=True)

        # Generate recommendations based on the most prevalent categories
        if 'Governance' in dict(sorted_categories) and dict(sorted_categories)['Governance'] > 0:
            logging.info("\n1. 🏛️ GOVERNANCE AND COMPLIANCE PRIORITY")
            logging.info("   Based on the high prevalence of governance-related themes, Tesla should:")
            logging.info("   • Strengthen anti-corruption and compliance frameworks.")
            logging.info("   • Enhance board governance and transparency reporting.")
            logging.info("   • Proactively address regulatory and legal framework concerns.")
            logging.info("   • Implement robust internal controls and audit processes.")

        if 'Economic' in dict(sorted_categories) and dict(sorted_categories)['Economic'] > 0:
            logging.info("\n2. 💼 FINANCIAL TRANSPARENCY AND STAKEHOLDER COMMUNICATION")
            logging.info("   Given the focus on stock market and economic themes, Tesla should:")
            logging.info("   • Improve financial disclosure and investor communications.")
            logging.info("   • Address executive compensation and shareholder concerns.")
            logging.info("   • Enhance quarterly earnings transparency on ESG metrics.")
            logging.info("   • Develop clearer ESG performance indicators for investors.")
            
        if 'Social' in dict(sorted_categories) and dict(sorted_categories)['Social'] > 0:
            logging.info("\n3. 👥 SOCIAL IMPACT AND WORKFORCE RELATIONS")
            logging.info("   Considering social themes, Tesla should focus on:")
            logging.info("   • Employee well-being and fair labor practices.")
            logging.info("   • Diversity, equity, and inclusion initiatives.")
            logging.info("   • Community engagement and responsible sourcing.")
            logging.info("   • Customer safety and product responsibility.")
            
        if 'Environmental' in dict(sorted_categories) and dict(sorted_categories)['Environmental'] > 0:
            logging.info("\n4. ♻️ ENVIRONMENTAL STEWARDSHIP AND INNOVATION")
            logging.info("   With environmental themes appearing, Tesla should:")
            logging.info("   • Emphasize sustainable manufacturing practices.")
            logging.info("   • Showcase advancements in renewable energy and clean technology.")
            logging.info("   • Provide transparent reporting on carbon footprint reduction.")
            logging.info("   • Invest in eco-friendly supply chain solutions.")

    def run_pipeline(self) -> None:
        """
        Runs the full ESG sentiment analysis pipeline:
        1. Loads data.
        2. Cleans and preprocesses data.
        3. Performs exploratory data analysis and visualization.
        4. Stores cleaned data to a SQLite database.
        5. Executes SQL queries for further analysis.
        6. Exports data for Business Intelligence tools.
        7. Conducts advanced analytics.
        8. Generates an executive summary.
        """
        logging.info("Starting Tesla ESG Sentiment Analysis Pipeline...")
        self._load_data()
        self.clean_and_preprocess_data()
        self.perform_eda()
        self.store_to_database()
        self.perform_sql_analysis()
        self.export_data_for_bi()
        self.perform_advanced_analytics()
        self.generate_executive_summary()
        logging.info("Tesla ESG Sentiment Analysis Pipeline completed successfully!")

if __name__ == "__main__":
    logging.info("Tesla ESG Sentiment Analysis - GDELT Dataset")
    logging.info("=" * 50)
    
    # Instantiate the analyzer (you can specify a different data_path or db_name if needed)
    analyzer = TeslaESGSentimentAnalyzer(data_path='data/tesla_esg.csv', db_name='tesla_esg.db')
    analyzer.run_pipeline()