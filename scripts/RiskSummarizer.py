# scripts/RiskSummarizer.py
from llm.LLMInterface import Chat, make_backend

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
    From these datasets, we have extracted the rows corresponding to locations that are located on 
    street segments that touch the address(es) provided. \n

    Below, I will provide the name of each dataset, a description of the dataset, and the extracted rows as mentioned above. \n\n
    """)   
    if data_handler:
        for ds in data_handler:
            prompt += (f"""
                Extracted data for dataset:{ds.name}:\n
                Which has the following description: {ds.description}\n
                Extracted rows: \n \n
                {ds.df.head(100).to_markdown(index=False)} \n \n
            """)
            print("length of extracted rows: ", len(ds.df))
    prompt += (f"""
        Based on the extracted data, please provide a detailed and accurate response to the users question: \n{user_text} \n.
    """)
    # print("Prompt for risk summarization: \n", prompt)
    if llm_chat:
        response = llm_chat.ask(prompt)
        print("Response from risk summarization: \n", response)
        return response
    else:
        chat = Chat(make_backend(provider="gemini"))
        chat.start()
        response = chat.ask(prompt)
        chat.reset()
        print("Response from risk summarization: \n", response)
    return response

