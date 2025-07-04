import pandas as pd

file_path = "data/20250703.gkg.csv"

cols_to_read = [1, 3, 6, 7, 9, 10]
col_names = ['SQLDATE', 'V2Themes', 'Organizations', 'V2Tone', 'SourceCollectionIdentifier', 'DocumentIdentifier']

df = pd.read_csv(
    file_path,
    delimiter='\t',
    header=None,
    usecols=cols_to_read,
    names=col_names,
    quoting=3,
    low_memory=False
)

# filter on either themes or organizations containing 'TESLA'
tesla_df = df[
    df['V2Themes'].str.contains("TESLA", case=False, na=False) |
    df['Organizations'].str.contains("TESLA", case=False, na=False)
]

print(f"Tesla ESG articles found: {len(tesla_df)}")
print(tesla_df.head())

tesla_df.to_csv("data/tesla_esg.csv", index=False)
print("✅ Filtered tesla_esg.csv saved including Organization matches")
