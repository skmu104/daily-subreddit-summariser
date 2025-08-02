from langchain.schema import Document
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
import asyncpraw
from datetime import datetime
import pandas as pd
import os
from pathlib import Path
from openai import AsyncOpenAI
import sys
import asyncio
from dotenv import load_dotenv

from map_reduce import MapReduce
from send_email import send_recording_to_email_list

load_dotenv()

async def scrape_subreddit(subreddit_name: str, limit=100) -> pd.DataFrame:
    client_id = os.environ.get("PRAW_CLIENT_ID")
    client_secret = os.environ.get("PRAW_CLIENT_SECRET")
    print(f"Client Id: {client_id}")
    print(f"Client Secret: {client_secret}")
    if not client_id or not client_secret:
        print("WARNING: PRAW client credentials not set. Ensure environment variables PRAW_CLIENT_ID and PRAW_CLIENT_SECRET")
        sys.exit(1)
    
    posts_data = []
    async with asyncpraw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent="nba_abridge v1.0 by /u/ndividual-Shake-904"
    ) as reddit:
        subreddit = await reddit.subreddit(subreddit_name)
        
        async for submission in subreddit.hot(limit=limit):
            if submission.stickied:
                continue
            
            author = submission.author
            post_data = {
                'title': submission.title,
                'score': submission.score,
                'id': submission.id,
                'url': submission.url,
                'num_comments': submission.num_comments,
                'created': datetime.fromtimestamp(submission.created_utc),
                'author': str(author) if author else '[deleted]',
                'selftext': submission.selftext,
                'subreddit': submission.subreddit.display_name,
                'upvote_ratio': submission.upvote_ratio
            }
            posts_data.append(post_data)
    
    return pd.DataFrame(posts_data)

async def get_posts() -> pd.DataFrame:
    print("Getting posts")
    df = await scrape_subreddit("nba", limit=50)
    df['is_highlight'] = df['url'].str.contains("https://streamable", na=False)
    shortened_df = df.sort_values(by='score',ascending=False).head(25)
    return shortened_df

def create_documents_from_reddit_data(posts: pd.DataFrame) -> list[Document]:
    documents = []
    for _, post in posts.iterrows():
        if post['is_highlight']:
            continue
        content = f"Title: {post.get('title', '')}\nDescription: {post.get('selftext', '')}"
        documents.append(Document(
            page_content=content,
        ))
    return documents

async def summarise_posts(documents: list[Document]) -> str:
    llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash-lite")
    map_chain, reduce_chain = create_map_reduce_chain(llm=llm)
    mr = MapReduce(llm, map_chain, reduce_chain)
    return await mr.execute(documents)

def create_map_reduce_chain(llm):
    map_template = """
Analyze the following content and extract key insights:

Content: {text}

1. Main topic or theme
2. Key points or insights
3. Important keywords

Analysis: 
"""
    map_prompt = PromptTemplate(template=map_template, input_variables=["text"])
    map_chain = map_prompt | llm | StrOutputParser()
    reduce_template = """
As a charasmatic sports reporter analyse the following insights and provide a 500 word summary:

Insights: {text}

1. Introduction text, glossing over the topics
2. Summary of each of the insights
3. Overall themes and patterns

Summary: 
"""
    reduce_prompt = PromptTemplate(template=reduce_template, input_variables=["text"])
    reduce_chain = reduce_prompt | llm | StrOutputParser()
    return map_chain, reduce_chain

async def create_audio(text: str) -> str:
    print("Converting summary to audio file")
    client = AsyncOpenAI()
    speech_file_path = Path(__file__).parent /  "artifacts/Daily Summary.mp3"
    async with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice="ballad",
        input=text,
        instructions="Speak like a sports presenter",
    ) as response:
        await response.stream_to_file(speech_file_path)

    return str(speech_file_path)

async def main():
    posts = await get_posts()
    print("Analysing and summarising subreddit data")
    documents = create_documents_from_reddit_data(posts)
    summary = await summarise_posts(documents)
    audio_file_path = await create_audio(summary)
    await send_recording_to_email_list(audio_file_path)

if __name__ == "__main__":
    asyncio.run(main())
