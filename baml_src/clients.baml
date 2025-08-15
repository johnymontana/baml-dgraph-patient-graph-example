client<llm> GPT4Mini {
  provider openai
  options {
    model "gpt-4o-mini"
    api_key env.OPENAI_API_KEY
    max_tokens 2000
    temperature 0.1
  }
}

client<llm> Claude {
  provider anthropic
  options {
    model "claude-3-haiku-20240307"
    api_key env.ANTHROPIC_API_KEY
    max_tokens 2000
    temperature 0.1
  }
}
