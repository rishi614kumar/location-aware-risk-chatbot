# scripts/RiskSummarizer.py
from llm.LLMInterface import Chat, make_backend
from config.logger import logger
from config.settings import DATASET_CONFIG

def summarize_risk(user_text, parsed_result, data_handler=None, llm_chat: Chat = None) -> str:
    """
    Stub for risk summarization. In production, this would use an LLM or rules to summarize risk.
    Args:
        user_text: str original user input text
        parsed_result: dict from the parser (categories, addresses, datasets, etc.)
        data_handler: DataHandler instance for full access to all queried datasets
    Returns:
        str: summary text
    """
    cats = parsed_result.get('categories', [])
    addresses = parsed_result.get('address', [])
    datasets = parsed_result.get('dataset_names', [])
    summary = [
        f"Risk summary for categories: {', '.join(cats) if cats else 'N/A'}.",
        f"Addresses: {', '.join(a.get('raw','') for a in addresses) if addresses else 'N/A'}.",
        f"Datasets: {', '.join(datasets) if datasets else 'N/A'}."
    ]
    prompt = (f"""
    You are a helpful assistant for NYC risk and compliance questions, related to building and construction risks.
    The user has asked the following question: \n{user_text} \n.
    Based on these questions, we have identified that the following categories are relevant: {', '.join(cats) if cats else 'N/A'} \n.
    Futhermore, we have identified that the question is related to the following address(es): \n
    {addresses} \n
    Furthermore, we have identified that the question is related to the following dataset(s): \n
    {datasets} \n
    These datasets are geo-referenced datasets, meaning that each row corresponds to a certain location.
    Depending on the dataset, we either extract the rows corresponding to the exact address(es) provided,
    or the rows corresponding to the surrounding area of the address(es) provided. Furthermore, each dataset
    has a specific geographic unit that is used to extract the rows. The geo-unit that is used to extract the
    rows will also be specified for each dataset below. \n

    Sometimes, a dataset may not have any rows corresponding to the exact address(es) provided, or
    even for the surrounding area. In this case, you will see no rows for that dataset. In that
    case, it is up to you to decide, based on the dataset's description, the user's question and your general knowledge, 
    whether to include the dataset in the summary or not. For example, for some datasets and a specific question, the fact
    that no rows are found for the exact address(es) provided or the surrounding area, may be very informative to the user's question.
    Whereas for other datasets and a specific question, the fact that no rows are found might not be informative and thus this should
    not be included in the summary.

    Furthermore, for each dataset, I will provide a pandas summary of the extracted rows. Only use this information if it is relevant.

    Below, I will provide the name of each dataset, a description of the dataset, whether the rows are extracted from the exact address(es) or the surrounding area,
     and the extracted rows as mentioned above. \n\n
    """)   
    if data_handler:
        for ds in data_handler:
            # Get scope information from DATASET_CONFIG
            # if not in DATASET_CONFIG, use default values (these are the defaults used in this case)
            ds_config = DATASET_CONFIG.get(ds.name, {}) 
            surrounding = ds_config.get("surrounding", False)
            mode = ds_config.get("mode", "street")
            geo_unit = ds_config.get("geo_unit", "BBL")
            
            # Determine scope description
            if surrounding:
                scope_desc = f"surrounding area (using {mode} mode with {geo_unit} filtering)"
            else:
                scope_desc = f"exact address(es) only (using {geo_unit} filtering)"
            
            prompt += (f"""
                Extracted data for dataset:{ds.name}:
                
                Which has the following description: {ds.description}
                Geographic scope: Data extracted from {scope_desc}
                Extracted rows: \n \n
                {ds.df.to_markdown(index=False)} \n \n
                Pandas summary of the extracted rows: \n \n
                {ds.df.describe().to_string() if len(ds.df) > 0 else "No rows found for this dataset"} \n \n
            """)
            logger.info(f"length of extracted rows for {ds.name}: {len(ds.df)}")
    prompt += (f"""
        Based on the extracted data, please provide a detailed and accurate response to the users question: \n{user_text} \n.
    """)
    if llm_chat:
        response = llm_chat.ask(prompt)
        logger.info(f"Response from risk summarization: {response}")
        return response
    else:
        chat = Chat(make_backend(provider="gemini"))
        chat.start()
        response = chat.ask(prompt)
        chat.reset()
        logger.info(f"Response from risk summarization: {response}")
    return response

