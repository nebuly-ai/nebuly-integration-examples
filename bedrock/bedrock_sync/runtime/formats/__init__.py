"""One adapter per request/response style.

The body style depends on the Bedrock API, not on the model: Converse and
ConverseStream share one shape for every model, while InvokeModel and
InvokeModelWithResponseStream carry the provider's native shape. Supporting a
new model family on InvokeModel means adding one module here.
"""
