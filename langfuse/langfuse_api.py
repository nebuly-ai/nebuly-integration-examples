# This scripts read all traces in a given time-range from langfuse, converts them to an Nebuly-compatible format and sends them to the Nebuly API.

import os
import requests
import json
import datetime

from dataclasses import dataclass


# Get the API key from the environment variable
private_api_key = os.getenv("LANGFUSE_PRIVATE_API_KEY")
public_api_key = os.getenv("LANGFUSE_PUBLIC_API_KEY")

nebuly_api_key = os.getenv("NEBULY_API_KEY")

# the time range to fetch traces from
start_date = datetime.datetime(2025, 1, 1)
end_date = datetime.datetime(2025, 1, 31)

NEBULY_URL = "https://backend.nebuly.com/event-ingestion/api/v2/events/trace_interaction"


@dataclass
class LLMTrace:
    messages: list[dict]
    time_start: str
    time_end: str
    model: str
    output: str
    input_tokens: int | None = None
    output_tokens: int | None = None

    def to_dict(self):
        return {
            "messages": self.messages,
            "time_start": self.time_start,
            "time_end": self.time_end,
            "model": self.model,
            "output": self.output,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens
        }


@dataclass
class RetrievalTrace:
    source: str
    input: str
    outputs: list[str]

    def to_dict(self):
        return {
            "source": self.source,
            "input": self.input,
            "outputs": self.outputs
        }


@dataclass
class Interaction:
    input: str
    output: str
    time_start: str
    time_end: str
    end_user: str
    tags: list[str]
    traces: list[RetrievalTrace | LLMTrace]

    @classmethod
    def from_langfuse_trace(cls, trace, observations):
        return cls(
            input=trace["input"],
            output=trace["output"],
            time_start=trace["timestamp"],
            time_end=trace["timestamp"],
            end_user=trace["userId"],
            tags=trace["tags"],
            traces=convert_observations_to_traces(observations)
        )


def get_traces(start_date, end_date, limit = 1000):
    page = 0
    full_traces = []
    while True:
        response = requests.get(
            "https://cloud.langfuse.com/api/public/traces",
            headers={
            "Authorization": f"Basic {public_api_key}:{private_api_key}"
            },
            params={
            "page": page,
            "limit": limit,
            "fromTimestamp": start_date,
            "toTimestamp": end_date,
            }
        )
        traces = response.json()["data"]
        if len(traces) == 0:
            break
        full_traces.extend(traces)
        page += 1
    return full_traces


def get_observations(trace_id, limit = 1000):
    page = 0
    full_observations = []
    while True:
        response = requests.get(
            "https://cloud.langfuse.com/api/public/observations",
            headers={
                "Authorization": f"Basic {public_api_key}:{private_api_key}"
            },
            params={
                "page": page,
                "limit": limit,
                "traceId": trace_id,
            }
        )
        response_json = response.json()
        observations = response_json["data"]
        meta = response_json["meta"]
        if int(meta["totalPages"]) == page:
            break
        if len(observations) == 0:
            break
        full_observations.extend(observations)
        page += 1
    return full_observations


def convert_observations_to_traces(observations):
    traces = []
    for observation in observations:
        if observation["parentObservationId"] is not None:
            # we only consider the "traces" in the main execution trace.
            continue
        if observation["model"] is not None:
            traces.append(LLMTrace(
                messages=json.loads(observation["input"]),
                time_start=observation["startTime"],
                time_end=observation["endTime"],
                model=observation["model"],
                output=observation["output"],
                input_tokens=observation["usageDetails"].get("input"),
                output_tokens=observation["usageDetails"].get("output")))
        else:
            traces.append(RetrievalTrace(
                source=observation["name"],
                input=observation["input"],
                outputs=[observation["output"]]
            ))
        
    return traces


def send_interactions_to_nebuly(interactions: list[Interaction]):
    for interaction in interactions:
        payload = {
            "interaction": interaction,
            "traces": [trace.to_dict() for trace in interaction.traces],
            "feedback_actions": [],
            "anonymize": True
        }
        requests.post(
            NEBULY_URL,
            headers={
                "Authorization": f"Bearer {nebuly_api_key}",
                "Content-Type": "application/json"
            },
            json=payload)


def main():
    traces = get_traces(start_date, end_date)
    nebuly_interactions = []
    for trace in traces:
        observations = get_observations(trace["id"])
        interaction = Interaction.from_langfuse_trace(trace, observations)
        nebuly_interactions.append(interaction)
    
    send_interactions_to_nebuly(nebuly_interactions)


if __name__ == "__main__":
    main()