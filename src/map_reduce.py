from langgraph.graph import END, START, StateGraph
import operator
from typing import Annotated, List, TypedDict
from langgraph.constants import Send
from IPython.display import Image


# This will be the overall state of the main graph.
# It will contain the input document contents, corresponding
# summaries, and a final summary.
class OverallState(TypedDict):
    # Notice here we use the operator.add
    # This is because we want combine all the summaries we generate
    # from individual nodes back into one list - this is essentially
    # the "reduce" part
    contents: List[str]
    summaries: Annotated[list, operator.add]
    final_summary: str


# This will be the state of the node that we will "map" all
# documents to in order to generate summaries
class SummaryState(TypedDict):
    content: str

class MapReduce():
    def __init__(self, llm, map_chain, reduce_chain):
        self.llm = llm
        self.map_chain = map_chain
        self.reduce_chain = reduce_chain
        self.construct_graph()
        pass

    # Here we generate a summary, given a document
    async def generate_summary(self,state: SummaryState):
        response = await self.map_chain.ainvoke(state["content"])
        return {"summaries": [response]}


    # Here we define the logic to map out over the documents
    # We will use this an edge in the graph
    def map_summaries(self, state: OverallState):
        # We will return a list of `Send` objects
        # Each `Send` object consists of the name of a node in the graph
        # as well as the state to send to that node
        return [
            Send("generate_summary", {"content": content}) for content in state["contents"]
        ]


    # Here we will generate the final summary
    async def generate_final_summary(self, state: OverallState):
        response = await self.reduce_chain.ainvoke(state["summaries"])
        return {"final_summary": response}

    def construct_graph(self):
        graph = StateGraph(OverallState)
        graph.add_node("generate_summary", self.generate_summary)
        graph.add_node("generate_final_summary", self.generate_final_summary)
        graph.add_conditional_edges(START, self.map_summaries, ["generate_summary"])
        graph.add_edge("generate_summary", "generate_final_summary")
        graph.add_edge("generate_final_summary", END)
        self.app = graph.compile()


    async def execute(self, documents) -> str:
        steps = self.app.astream({"contents": [doc.page_content for doc in documents]})
        last_step = {}
        async for step in steps:
            last_step = step
        return last_step['generate_final_summary']['final_summary']