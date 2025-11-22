import asyncio
import os
import time  # added for per-query timing

import chainlit as cl
from chainlit.data.sql_alchemy import SQLAlchemyDataLayer
from llm.LLMInterface import Chat, make_backend
from prompts.app_prompts import get_first_message, get_system_prompt
from config.logger import logger
from scripts.ConversationalAgent import ConversationalAgent
from config.settings import CHATBOT_TYPEWRITER_DELAY

# LLM for conversational response
llm_chat = Chat(make_backend(provider="gemini"))
# Persistent agent instance
agent = ConversationalAgent(chat_backend=llm_chat)


def _initialize_agent() -> None:
    """Reset agent state and prime the chat backend with the system prompt."""
    system_prompt = get_system_prompt()
    agent.llm_chat.start(system_instruction=system_prompt)
    agent.chat_history = []
    agent.last_parsed_result = None
    agent.last_context = None


def _restore_chat_history_from_thread(thread) -> None:
    """Populate the agent's chat history with prior user turns from a stored thread."""
    steps = (thread or {}).get("steps") or []
    history = []
    for step in steps:
        if step.get("type") == "user_message":
            text = step.get("output") or step.get("input")
            if text:
                history.append(text)
    agent.chat_history = history
    logger.info("Restored %d prior user turns into agent history.", len(history))


def _ensure_sqlite_schema(data_layer: SQLAlchemyDataLayer) -> None:
    """Create persistence tables when running against a local SQLite database."""

    ddl_statements = [
        "PRAGMA foreign_keys=ON",
        """
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            identifier TEXT NOT NULL UNIQUE,
            metadata TEXT NOT NULL,
            createdAt TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS threads (
            id TEXT PRIMARY KEY,
            createdAt TEXT,
            name TEXT,
            userId TEXT,
            userIdentifier TEXT,
            tags TEXT,
            metadata TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS steps (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            threadId TEXT NOT NULL,
            parentId TEXT,
            streaming INTEGER NOT NULL,
            waitForAnswer INTEGER,
            isError INTEGER,
            metadata TEXT,
            tags TEXT,
            input TEXT,
            output TEXT,
            createdAt TEXT,
            command TEXT,
            start TEXT,
            end TEXT,
            generation TEXT,
            showInput TEXT,
            language TEXT,
            indent INTEGER,
            defaultOpen INTEGER,
            FOREIGN KEY(threadId) REFERENCES threads(id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS elements (
            id TEXT PRIMARY KEY,
            threadId TEXT,
            type TEXT,
            url TEXT,
            chainlitKey TEXT,
            name TEXT NOT NULL,
            display TEXT,
            objectKey TEXT,
            size TEXT,
            page INTEGER,
            language TEXT,
            forId TEXT,
            mime TEXT,
            props TEXT,
            FOREIGN KEY(threadId) REFERENCES threads(id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS feedbacks (
            id TEXT PRIMARY KEY,
            forId TEXT NOT NULL,
            threadId TEXT NOT NULL,
            value INTEGER NOT NULL,
            comment TEXT,
            FOREIGN KEY(threadId) REFERENCES threads(id) ON DELETE CASCADE,
            FOREIGN KEY(forId) REFERENCES steps(id) ON DELETE CASCADE
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_threads_user_identifier ON threads(userIdentifier)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_steps_thread ON steps(threadId)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_elements_thread ON elements(threadId)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_feedbacks_thread ON feedbacks(threadId)
        """
    ]

    async def _create_tables():
        for statement in ddl_statements:
            await data_layer.execute_sql(statement, {})

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_create_tables())
    finally:
        loop.close()
        asyncio.set_event_loop(None)


@cl.data_layer
def provide_data_layer():
    """Expose a SQLAlchemy-backed data layer for thread persistence."""
    conninfo = os.getenv("CHAINLIT_DB_URL", "sqlite+aiosqlite:///./chainlit_history.db")
    data_layer = SQLAlchemyDataLayer(conninfo=conninfo)
    if conninfo.startswith("sqlite"):
        _ensure_sqlite_schema(data_layer)
    return data_layer


@cl.password_auth_callback
def password_auth(username: str, password: str):
    expected_user = os.getenv("CHAINLIT_ADMIN_USER")
    expected_password = os.getenv("CHAINLIT_ADMIN_PASSWORD")
    if not expected_user or not expected_password:
        logger.error("Authentication credentials not configured; denying login attempt.")
        return None
    if username == expected_user and password == expected_password:
        logger.info("User %s authenticated successfully.", username)
        return cl.User(identifier=username, metadata={"role": "admin"})
    logger.warning("Invalid credentials supplied for user %s.", username)
    return None

@cl.on_chat_start
async def start():
    logger.info('Chat started')
    _initialize_agent()
    await cl.Message(get_first_message()).send()


@cl.on_chat_resume
async def on_chat_resume(thread):
    thread_id = (thread or {}).get("id")
    logger.info("Resuming chat: %s", thread_id)
    _initialize_agent()
    _restore_chat_history_from_thread(thread)

@cl.on_message
async def on_message(msg: cl.Message):
    start_ts = time.perf_counter()  # start timer
    user_text = msg.content
    logger.info(f'Received message: {user_text}')
    # Create a single message and stream tokens for typewriter effect
    streamed_msg = cl.Message(content="")
    await streamed_msg.send()
    async for chunk in agent.stream(user_text):
        if CHATBOT_TYPEWRITER_DELAY:
            for token in chunk:
                await streamed_msg.stream_token(token)
                await asyncio.sleep(CHATBOT_TYPEWRITER_DELAY)
        else:
            await streamed_msg.stream_token(chunk)
        if not chunk.endswith("\n"):
            await streamed_msg.stream_token("\n\n")
    await streamed_msg.update()
    elapsed = time.perf_counter() - start_ts
    logger.info(f"Query completed in {elapsed:.3f}s")

    # diagnostic log block
    ctx = agent.last_context or {}
    parsed = ctx.get("parsed_result", {})
    decisions = {
        "mode": ctx.get("mode"),
        "reuse_addresses_decision": ctx.get("reuse_addresses_decision"),
        "reuse_datasets_decision": ctx.get("reuse_datasets_decision"),
        "surrounding_decision": ctx.get("surrounding_decision"),
        "risk_decision": ctx.get("risk_decision"),
        "show_data_decision": ctx.get("show_data_decision"),
    }
    dataset_filters = ctx.get("dataset_filters", {})
    filtered_datasets = ctx.get("filtered_datasets", [])
    data_samples = ctx.get("data_samples", {})

    lengths = {name: (getattr(df, 'shape', (len(df) if hasattr(df, '__len__') else None, None))[0] if df is not None else 0) for name, df in data_samples.items()}

    logger.info(
        "QUERY DIAGNOSTICS | decisions=%s | parsed_categories=%s | parsed_datasets=%s | addresses=%s | confidence=%s | filters=%s | dataset_lengths=%s | elapsed=%.3fs",
        decisions,
        parsed.get('categories'),
        parsed.get('dataset_names'),
        parsed.get('address'),
        parsed.get('confidence'),
        dataset_filters,
        lengths,
        elapsed
    )
    '''--- Previous implementation without ConversationalAgent ---'''
    '''# 2. Parse for structured info
    result = route_query_to_datasets_multi(user_text)
    logger.info(f'Parsed result: {result}')
    categories = result.get('categories', [])
    datasets = result.get('dataset_names', [])
    addresses = result.get('address', [])
    confidence = result.get('confidence')

    # --- Robust conversational logic ---
    # If no address and no relevant datasets/categories, continue conversation only
    if not addresses and not datasets and not categories:
        from prompts.app_prompts import get_conversational_fallback_prompt
        followup = llm_chat.ask(get_conversational_fallback_prompt())
        await cl.Message(content=followup).send()
        return

    # Only show parsing output if there is something to show
    if categories or datasets or addresses:
        cat_str = '\n'.join(f'- {c}' for c in categories) or 'None'
        ds_str = '\n'.join(f'- {d}' for d in datasets) or 'None'
        if addresses:
            addr_str = '\n\n'.join(
                '\n'.join([
                    f"  House Number: {a.get('house_number', '')}",
                    f"  Street Name:  {a.get('street_name', '')}",
                    f"  Borough:      {a.get('borough', '')}",
                    f"  Raw:          {a.get('raw', '')}",
                    f"  Notes:        {a.get('notes', '')}"
                ]) for a in addresses
            )
        else:
            addr_str = 'None'

        formatted = (
            f"**Categories:**\n{cat_str}\n\n"
            f"**Datasets:**\n{ds_str}\n\n"
            f"**Addresses:**\n{addr_str}\n\n"
            f"**Confidence:** {confidence}"
        )
        logger.info(f'Formatted parsing output: {formatted}')
        await cl.Message(content=formatted).send()
    
    # 3. Data preview for each dataset
    handler = DataHandler(datasets)
    logger.info(f"Handler initialized with datasets: {handler.names}")
    
    # Get filtering logic from GeoScope 
    llm_data_response = llm_chat.ask(get_loading_datasets_prompt(handler))
    logger.info(f'LLM Data Loading Response: {llm_data_response}')
    await cl.Message(content=f"{llm_data_response}").send()
    await asyncio.sleep(0)

    try:
        logger.info(f"Calling get_dataset_filters with addresses: {addresses}")
        dataset_filters, geo_bundles = await asyncio.to_thread(get_dataset_filters, addresses, handler)
        setattr(handler, "geo_bundles", geo_bundles)
        logger.info(
            "Received dataset_filters: %s (geo bundles=%d)",
            dataset_filters,
            len(geo_bundles),
        )
    except Exception as e:
        logger.error(f"Error in get_dataset_filters: {e}")
        await cl.Message(content=f"Error in GeoScope filtering: {e}").send()
        await asyncio.sleep(0)
        dataset_filters = {}

    
    data_samples = {}
    filtered_datasets = []
    for ds in handler:
        try:
            logger.info(f"Processing dataset: {ds.name}")
            filter_kwargs = dataset_filters.get(ds.name, {})
            where = filter_kwargs.get("where")
            limit = filter_kwargs.get("limit")
            logger.info(f"Filter for {ds.name}: where={where}, limit={limit}")
            df_full = await asyncio.to_thread(ds.df_filtered, where, limit)
            logger.info(f"Fetched dataframe for {ds.name}, shape: {getattr(df_full, 'shape', None)}")
            df_head = df_full.head(5)
            # Replace DataSet's _df_cache with filtered data if filtering was applied
            if where is not None or limit is not None:
                object.__setattr__(ds, "_df_cache", df_full)
            data_samples[ds.name] = df_head
            filtered_datasets.append(ds)
            preview = df_head.to_markdown(index=False) if hasattr(df_head, 'to_markdown') else str(df_head)
            logger.info(f'Dataset {ds.name} loaded successfully')
            await cl.Message(content=f"**{ds.name}**\n{ds.description}\n\nPreview:\n{preview}").send()
            await asyncio.sleep(0)
        except Exception as e:
            logger.error(f'Error loading dataset {ds.name}: {e}')
            await cl.Message(content=f"**{ds.name}**\n{ds.description}\n\nError loading data: {e}").send()
            await asyncio.sleep(0)
    # Replace handler with filtered datasets
    handler._datasets = filtered_datasets

    # 4. Risk summarization
    try:
        risk_summary = await asyncio.to_thread(summarize_risk, user_text, result, handler, llm_chat)
        logger.info(f'Risk summary: {risk_summary}')
        await cl.Message(content=f"**Risk Summary:**\n{risk_summary}").send()
        await asyncio.sleep(0)
    except Exception as e:
        logger.error(f"Error in risk summarization: {e}")
        await cl.Message(content=f"Error in risk summarization: {e}").send()
        await asyncio.sleep(0)

    # 5. Conversational follow-up
    try:
        followup_prompt = get_followup_prompt(result, risk_summary)
        logger.info(f'Followup prompt: {followup_prompt}')
        followup_response = await asyncio.to_thread(llm_chat.ask, followup_prompt)
        logger.info(f'Followup response: {followup_response}')
        await cl.Message(content=followup_response).send()
        await asyncio.sleep(0)
    except Exception as e:
        logger.error(f"Error in followup response: {e}")
        await cl.Message(content=f"Error in followup response: {e}").send()
        await asyncio.sleep(0)'''