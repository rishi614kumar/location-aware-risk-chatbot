import chainlit as cl
from llm.LLMParser import route_query_to_datasets_multi
from scripts.DataHandler import DataHandler
from llm.LLMInterface import Chat, make_backend
from scripts.RiskSummarizer import summarize_risk
from prompts.app_prompts import get_first_message, get_system_prompt, get_conversational_meta_prompt, get_followup_prompt
from scripts.GeoScope import get_dataset_filters
import logging

# LLM for conversational response
llm_chat = Chat(make_backend(provider="gemini"))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

@cl.on_chat_start
async def start():
    logger.info('Chat started')
    # Set a system context for the LLM chat (imported from prompts.py)
    llm_chat.start(system_instruction=get_system_prompt())
    await cl.Message(get_first_message()).send()

@cl.on_message
async def on_message(msg: cl.Message):
    user_text = msg.content
    logger.info(f'Received message: {user_text}')
    # 1. Conversational LLM response
    llm_response = llm_chat.ask(get_conversational_meta_prompt(user_text))
    logger.info(f'LLM response: {llm_response}')
    await cl.Message(content=f"{llm_response}").send()

    # 2. Parse for structured info
    result = route_query_to_datasets_multi(user_text)
    logger.info(f'Parsed result: {result}')
    categories = result.get('categories', [])
    datasets = result.get('dataset_names', [])
    addresses = result.get('address', [])
    confidence = result.get('confidence')

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
    # Get filtering logic from GeoScope stub
    dataset_filters = get_dataset_filters(addresses, handler)
    data_samples = {}
    filtered_datasets = []
    for ds in handler:
        try:
            filter_kwargs = dataset_filters.get(ds.name, {})
            where = filter_kwargs.get("where")
            limit = filter_kwargs.get("limit")
            df_full = ds.df_filtered(where=where, limit=limit)
            df_head = df_full.head(5)
            # Replace DataSet's _df_cache with filtered data if filtering was applied
            if where is not None or limit is not None:
                object.__setattr__(ds, "_df_cache", df_full)
            data_samples[ds.name] = df_head
            filtered_datasets.append(ds)
            preview = df_head.to_markdown(index=False) if hasattr(df_head, 'to_markdown') else str(df_head)
            logger.info(f'Dataset {ds.name} loaded successfully')
            await cl.Message(content=f"**{ds.name}**\n{ds.description}\n\nPreview:\n{preview}").send()
        except Exception as e:
            logger.error(f'Error loading dataset {ds.name}: {e}')
            await cl.Message(content=f"**{ds.name}**\n{ds.description}\n\nError loading data: {e}").send()
    # Replace handler with filtered datasets
    handler._datasets = filtered_datasets

    # 4. Risk summarization
    risk_summary = summarize_risk(result, handler)
    logger.info(f'Risk summary: {risk_summary}')
    await cl.Message(content=f"**Risk Summary:**\n{risk_summary}").send()

    # 5. Conversational follow-up
    followup_prompt = get_followup_prompt(result, risk_summary)
    logger.info(f'Followup prompt: {followup_prompt}')
    followup_response = llm_chat.ask(followup_prompt)
    logger.info(f'Followup response: {followup_response}')
    await cl.Message(content=followup_response).send()