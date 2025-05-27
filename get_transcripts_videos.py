import polars as pl
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound, VideoUnavailable
import sys

def extract(transcript: list) -> str:
    return ' '.join([transcript[i]['text'] for i in range(len(transcript))])

df = pl.read_parquet('data/video-ids.parquet')
sys.stdout.reconfigure(encoding='utf-8')
print(df.head())

transcript_text_list = []

for video_id in df['video_id'].to_list():
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        text = extract(transcript)
        
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as e:
        print(f"Transcript not available for video ID {video_id}: {e}")
        text = "n/a"
    except Exception as e:
        print(f"Unexpected error for video ID {video_id}: {e}")
        text = "n/a"
    transcript_text_list.append(text)

df = df.with_columns(pl.Series(name="transcript", values=transcript_text_list))
print(df.head())

df.write_parquet('data/video-transcripts.parquet')
df.write_csv('data/video-transcripts.csv')