import polars as pl
import matplotlib.pyplot as plt
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


df = pl.read_parquet('data/video-transcripts.parquet')

print("shape:", df.shape)
print("n unique rows:", df.n_unique())
for j in range(df.shape[1]):
    print("n unique elements (" + df.columns[j] + "):", df[:,j].n_unique())
    print("n null elements:", df[:j].null_count().sum())

df = df.with_columns(pl.col('datetime').cast(pl.Datetime))
print(df.head())

plt.hist(df['title'].str.len_chars())
plt.hist(df['transcript'].str.len_chars())

print(df['title'][3])
print(df['transcript'][3])

special_strings = ['&#39;', '&amp;', 'sha '] #&#39; → representa o apóstrofo (') em HTML; &amp; → representa o e comercial (&) em HTML
special_string_replacements = ["'", "&", "Shaw "]

for i in range(len(special_strings)):
    df = df.with_columns(df['title'].str.replace(special_strings[i], special_string_replacements[i]).alias('title'))
    df = df.with_columns(df['transcript'].str.replace(special_strings[i], special_string_replacements[i]).alias('transcript'))

print(df['title'][3])
print(df['transcript'][3])

df.write_parquet('data/video-transcripts.parquet')
df.write_csv('data/video-transcripts.csv')