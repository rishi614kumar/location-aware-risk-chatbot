import chainlit as cl

@cl.on_chat_start
async def start():
    await cl.Message("enter input text").send()

@cl.on_message
async def on_message(msg: cl.Message):
    await cl.Message(content=f"text sent {msg.content}").send()