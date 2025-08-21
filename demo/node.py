# from langfuse.decorators import observe, langfuse_context
from pocket import Node,Flow,build_mermaid,EXEC

import time

class SummarizeFile(Node):
    def prep(self, shared):
        return shared["data"]

    def exec(self, prep_res):
        if not prep_res:
            return "Empty file content"

        time.sleep(2)
        
        prompt = f"Summarize this text in 10 words: {prep_res}"
        summary = prompt #call_llm(prompt)  # might fail
        return summary

    def exec_fallback(self, prep_res, exc):
        # Provide a simple fallback instead of crashing
        return "There was an error processing your request."

    def post(self, shared, prep_res, exec_res):
        shared["summary"] = exec_res
        # Return "default" by not returning


#####################################################
# https://the-pocket.github.io/PocketFlow/
#
def pocketflow(shared,headers={},doc=False):
    
    summarize_node = SummarizeFile(max_retries=3)

    # node.run() calls prep->exec->post
    # If exec() fails, it retries up to 3 times before calling exec_fallback()

    flow = summarize_node
    # flow = Flow(start=step1)
    
    docs = {"data":"content"}
    
    return EXEC(flow,docs,shared,doc)